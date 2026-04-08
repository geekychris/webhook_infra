package com.eventconduit.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Manages WebSocket connections to the Event Conduit stream gateway.
 *
 * Position tracking:
 *   - Each event is received, then dispatched to listeners.
 *   - After listener processing, the position is marked as processed in the
 *     PositionTracker (persisted to disk).
 *   - ACK is sent to the server only AFTER processing + persisting.
 *   - On reconnect, resume_after is set to the last persisted position.
 *
 * Auto-reconnect:
 *   - When the connection drops unexpectedly, the service waits 2 seconds
 *     then reconnects from the last processed position.
 *   - Manual disconnect() disables auto-reconnect.
 */
@Service
public class StreamConsumerService {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumerService.class);
    private static final int MAX_EVENTS = 500;

    private final ObjectMapper mapper = new ObjectMapper();
    private final PositionTracker positionTracker;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private final AtomicLong totalReceived = new AtomicLong(0);

    private final LinkedList<ReceivedEvent> events = new LinkedList<>();
    private final List<Consumer<ReceivedEvent>> eventListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<ConnectionState>> stateListeners = new CopyOnWriteArrayList<>();

    private volatile WebSocketClient client;
    private volatile String currentSubscriptionId;
    private volatile String currentGatewayUrl;
    private volatile String currentToken;
    private volatile boolean autoReconnect = false;
    private volatile String statusMessage = "Not connected";

    public enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, ERROR, RECONNECTING
    }

    public StreamConsumerService(PositionTracker positionTracker) {
        this.positionTracker = positionTracker;
    }

    /**
     * Connect to the stream gateway. Enables auto-reconnect.
     */
    public void connect(String gatewayUrl, String subscriptionId, String token, long resumeAfter) {
        autoReconnect = true;
        currentGatewayUrl = gatewayUrl;
        currentSubscriptionId = subscriptionId;
        currentToken = token;

        // If no explicit resume position, use the persisted one.
        if (resumeAfter <= 0) {
            resumeAfter = positionTracker.getLastProcessed(subscriptionId);
            if (resumeAfter > 0) {
                log.info("Resuming from persisted position {} for subscription {}", resumeAfter, subscriptionId);
            }
        }

        doConnect(resumeAfter);
    }

    /**
     * Disconnect and disable auto-reconnect.
     */
    public void disconnect() {
        autoReconnect = false;
        if (client != null) {
            try { client.close(); } catch (Exception e) { /* ignore */ }
            client = null;
        }
        setState(ConnectionState.DISCONNECTED);
        statusMessage = "Disconnected";
    }

    public void clearEvents() {
        synchronized (events) { events.clear(); }
        totalReceived.set(0);
    }

    // --- Accessors ---

    public ConnectionState getState() { return state.get(); }
    public String getStatusMessage() { return statusMessage; }
    public long getTotalReceived() { return totalReceived.get(); }
    public String getCurrentSubscriptionId() { return currentSubscriptionId; }

    public long getLastProcessedPosition() {
        return currentSubscriptionId != null
            ? positionTracker.getLastProcessed(currentSubscriptionId)
            : 0;
    }

    public List<ReceivedEvent> getEvents() {
        synchronized (events) { return new ArrayList<>(events); }
    }

    // --- Listeners ---

    public void addEventListener(Consumer<ReceivedEvent> listener) { eventListeners.add(listener); }
    public void removeEventListener(Consumer<ReceivedEvent> listener) { eventListeners.remove(listener); }
    public void addStateListener(Consumer<ConnectionState> listener) { stateListeners.add(listener); }
    public void removeStateListener(Consumer<ConnectionState> listener) { stateListeners.remove(listener); }

    // --- Internal ---

    private void doConnect(long resumeAfter) {
        if (client != null) {
            try { client.close(); } catch (Exception e) { /* ignore */ }
        }

        String url = currentGatewayUrl
            + "?subscription_id=" + currentSubscriptionId
            + "&token=" + currentToken;
        if (resumeAfter > 0) {
            url += "&resume_after=" + resumeAfter;
        }

        setState(ConnectionState.CONNECTING);
        statusMessage = "Connecting...";
        log.info("Connecting: subscription={}, resume_after={}", currentSubscriptionId, resumeAfter);

        try {
            client = new EventConduitWSClient(URI.create(url));
            client.connect();
        } catch (Exception e) {
            setState(ConnectionState.ERROR);
            statusMessage = "Connection failed: " + e.getMessage();
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!autoReconnect) return;
        setState(ConnectionState.RECONNECTING);
        statusMessage = "Reconnecting in 2s...";
        log.info("Scheduling reconnect for subscription {}", currentSubscriptionId);

        scheduler.schedule(() -> {
            if (!autoReconnect) return;
            long resumeFrom = positionTracker.getLastProcessed(currentSubscriptionId);
            doConnect(resumeFrom);
        }, 2, TimeUnit.SECONDS);
    }

    private void setState(ConnectionState newState) {
        state.set(newState);
        for (var l : stateListeners) { try { l.accept(newState); } catch (Exception e) { /* ignore */ } }
    }

    private void handleMessage(String raw) {
        try {
            StreamMessage msg = mapper.readValue(raw, StreamMessage.class);
            if (!"event".equals(msg.type())) return;

            CloudEvent ce = mapper.treeToValue(msg.event(), CloudEvent.class);

            ReceivedEvent received = new ReceivedEvent(
                msg.position(), ce.id(), ce.type(), ce.source(), ce.subject(),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(msg.event()),
                Instant.now()
            );

            // Store in the event buffer.
            synchronized (events) {
                events.addFirst(received);
                while (events.size() > MAX_EVENTS) events.removeLast();
            }
            totalReceived.incrementAndGet();

            // Notify application listeners (this is the "processing" step).
            for (var listener : eventListeners) {
                try { listener.accept(received); } catch (Exception e) { /* ignore */ }
            }

            // Mark as processed AFTER listeners have run, then ACK.
            positionTracker.markProcessed(currentSubscriptionId, msg.position());
            sendAck(msg.position());

        } catch (Exception e) {
            log.warn("Failed to handle stream message", e);
        }
    }

    private void sendAck(long position) {
        if (client != null && client.isOpen()) {
            try {
                client.send(mapper.writeValueAsString(Map.of("type", "ack", "position", position)));
            } catch (Exception e) {
                log.warn("ACK failed for position {}", position, e);
            }
        }
    }

    private class EventConduitWSClient extends WebSocketClient {
        public EventConduitWSClient(URI uri) { super(uri); }

        @Override
        public void onOpen(ServerHandshake handshake) {
            log.info("Stream connected: subscription={}", currentSubscriptionId);
            setState(ConnectionState.CONNECTED);
            statusMessage = "Connected — subscription " + currentSubscriptionId;
        }

        @Override
        public void onMessage(String message) {
            handleMessage(message);
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            log.info("Stream closed: code={}, reason={}, remote={}", code, reason, remote);
            if (remote && autoReconnect) {
                statusMessage = "Connection lost, reconnecting...";
                scheduleReconnect();
            } else {
                setState(ConnectionState.DISCONNECTED);
                statusMessage = "Disconnected" + (reason != null && !reason.isEmpty() ? ": " + reason : "");
            }
        }

        @Override
        public void onError(Exception ex) {
            log.error("Stream error", ex);
            setState(ConnectionState.ERROR);
            statusMessage = "Error: " + ex.getMessage();
            scheduleReconnect();
        }
    }
}
