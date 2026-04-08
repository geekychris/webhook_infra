package com.eventconduit.consumer;

import java.time.Instant;

/**
 * An event received from the stream, enriched with receive metadata.
 */
public record ReceivedEvent(
    long position,
    String eventId,
    String eventType,
    String source,
    String subject,
    String rawJson,
    Instant receivedAt
) {}
