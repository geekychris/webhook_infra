package com.eventconduit.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the last successfully processed position per subscription.
 * Persists to a local file so the position survives JVM restarts.
 *
 * File format: one line per subscription: "subscription_id=position"
 * Default path: ./data/stream-positions.txt
 */
@Component
public class PositionTracker {

    private static final Logger log = LoggerFactory.getLogger(PositionTracker.class);
    private static final Path POSITIONS_FILE = Path.of("data", "stream-positions.txt");

    private final ConcurrentHashMap<String, AtomicLong> positions = new ConcurrentHashMap<>();

    public PositionTracker() {
        loadFromDisk();
    }

    /**
     * Record that the given position has been successfully processed.
     * This is the position that will be used as resume_after on reconnect.
     */
    public void markProcessed(String subscriptionId, long position) {
        positions.computeIfAbsent(subscriptionId, k -> new AtomicLong(0))
                 .accumulateAndGet(position, Math::max); // only advance, never go backwards
        persistToDisk();
    }

    /**
     * Get the last processed position for a subscription.
     * Returns 0 if no position has been tracked.
     */
    public long getLastProcessed(String subscriptionId) {
        AtomicLong pos = positions.get(subscriptionId);
        return pos != null ? pos.get() : 0;
    }

    private void loadFromDisk() {
        if (!Files.exists(POSITIONS_FILE)) return;
        try {
            for (String line : Files.readAllLines(POSITIONS_FILE)) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    try {
                        positions.put(parts[0].trim(), new AtomicLong(Long.parseLong(parts[1].trim())));
                    } catch (NumberFormatException e) {
                        // skip bad lines
                    }
                }
            }
            log.info("Loaded {} stream positions from {}", positions.size(), POSITIONS_FILE);
        } catch (IOException e) {
            log.warn("Could not load stream positions from {}: {}", POSITIONS_FILE, e.getMessage());
        }
    }

    private void persistToDisk() {
        try {
            Files.createDirectories(POSITIONS_FILE.getParent());
            StringBuilder sb = new StringBuilder();
            sb.append("# Event Conduit stream positions (subscription_id=last_processed_position)\n");
            positions.forEach((subId, pos) -> sb.append(subId).append("=").append(pos.get()).append("\n"));
            Files.writeString(POSITIONS_FILE, sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.warn("Could not persist stream positions to {}: {}", POSITIONS_FILE, e.getMessage());
        }
    }
}
