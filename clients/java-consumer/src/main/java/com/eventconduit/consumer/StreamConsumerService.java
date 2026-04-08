package com.eventconduit.consumer;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Manages WebSocket connections to the Event Conduit stream gateway.
 * Supports connect/disconnect, auto-ACK, resume, and event listeners.
 */
@Service
public class StreamConsumerService {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumerService.class);
    private static final int MAX_EVENTS = 500;

    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private final AtomicLong totalReceived = new AtomicLong(0);
    private final AtomicLong lastPosition = new AtomicLong(0);

    private final LinkedList<ReceivedEvent> events = new LinkedList<>();
    private final List<Consumer<ReceivedEvent>> eventListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<ConnectionState>> stateListeners = new CopyOnWriteArrayList<>();

    private volatile WebSocketClient client;
    private volatile String currentSubscriptionId;
    private volatile String currentGatewayUrl;
    private volatile String statusMessage = "Not connected";

    public enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, ERROR
    }

    /**
     * Connect to the stream gateway.
     */
    public void connect(String gatewayUrl, String subscriptionId, String token, long resumeAfter) {
        disconnect();

        currentGatewayUrl = gatewayUrl;
        currentSubscriptionId = subscriptionId;

        String url = gatewayUrl
            + "?subscription_id=" + subscriptionId
            + "&token=" + token;
        if (resumeAfter > 0) {
            url += "&resume_after=" + resumeAfter;
        }

        setState(ConnectionState.CONNECTING);
        statusMessage = "Connecting to " + gatewayUrl + "...";
        log.info("Connecting to stream: subscription={}, gateway={}", subscriptionId, gatewayUrl);

        try {
            client = new EventConduitWebSocketClient(URI.create(url));
            client.connect();
        } catch (Exception e) {
            setState(ConnectionState.ERROR);
            statusMessage = "Connection failed: " + e.getMessage();
            log.error("Connection failed", e);
        }
    }

    /**
     * Disconnect from the stream gateway.
     */
    public void disconnect() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Error closing WebSocket", e);
            }
            client = null;
        }
        setState(ConnectionState.DISCONNECTED);
        statusMessage = "Disconnected";
    }

    /**
     * Clear all received events.
     */
    public void clearEvents() {
        synchronized (events) {
            events.clear();
        }
        totalReceived.set(0);
    }

    // --- Accessors ---

    public ConnectionState getState() { return state.get(); }
    public String getStatusMessage() { return statusMessage; }
    public long getTotalReceived() { return totalReceived.get(); }
    public long getLastPosition() { return lastPosition.get(); }
    public String getCurrentSubscriptionId() { return currentSubscriptionId; }

    public List<ReceivedEvent> getEvents() {
        synchronized (events) {
            return new ArrayList<>(events);
        }
    }

    // --- Listeners ---

    public void addEventListener(Consumer<ReceivedEvent> listener) {
        eventListeners.add(listener);
    }

    public void removeEventListener(Consumer<ReceivedEvent> listener) {
        eventListeners.remove(listener);
    }

    public void addStateListener(Consumer<ConnectionState> listener) {
        stateListeners.add(listener);
    }

    public void removeStateListener(Consumer<ConnectionState> listener) {
        stateListeners.remove(listener);
    }

    // --- Internal ---

    private void setState(ConnectionState newState) {
        state.set(newState);
        for (var listener : stateListeners) {
            try { listener.accept(newState); } catch (Exception e) { /* ignore */ }
        }
    }

    private void handleMessage(String raw) {
        try {
            StreamMessage msg = mapper.readValue(raw, StreamMessage.class);
            if (!"event".equals(msg.type())) return;

            // Parse the CloudEvent from the event field
            CloudEvent ce = mapper.treeToValue(msg.event(), CloudEvent.class);

            ReceivedEvent received = new ReceivedEvent(
                msg.position(),
                ce.id(),
                ce.type(),
                ce.source(),
                ce.subject(),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(msg.event()),
                Instant.now()
            );

            synchronized (events) {
                events.addFirst(received);
                while (events.size() > MAX_EVENTS) {
                    events.removeLast();
                }
            }

            totalReceived.incrementAndGet();
            lastPosition.set(msg.position());

            // Send ACK
            sendAck(msg.position());

            // Notify listeners
            for (var listener : eventListeners) {
                try { listener.accept(received); } catch (Exception e) { /* ignore */ }
            }

        } catch (Exception e) {
            log.warn("Failed to parse stream message", e);
        }
    }

    private void sendAck(long position) {
        if (client != null && client.isOpen()) {
            try {
                String ack = mapper.writeValueAsString(Map.of("type", "ack", "position", position));
                client.send(ack);
            } catch (Exception e) {
                log.warn("Failed to send ACK for position {}", position, e);
            }
        }
    }

    /**
     * Inner WebSocket client that delegates events to the service.
     */
    private class EventConduitWebSocketClient extends WebSocketClient {

        public EventConduitWebSocketClient(URI serverUri) {
            super(serverUri);
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            log.info("Stream connected: subscription={}", currentSubscriptionId);
            setState(ConnectionState.CONNECTED);
            statusMessage = "Connected to " + currentSubscriptionId;
        }

        @Override
        public void onMessage(String message) {
            handleMessage(message);
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            log.info("Stream disconnected: code={}, reason={}, remote={}", code, reason, remote);
            setState(ConnectionState.DISCONNECTED);
            statusMessage = "Disconnected" + (reason != null && !reason.isEmpty() ? ": " + reason : "");
        }

        @Override
        public void onError(Exception ex) {
            log.error("Stream error", ex);
            setState(ConnectionState.ERROR);
            statusMessage = "Error: " + ex.getMessage();
        }
    }
}
