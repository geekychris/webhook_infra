package com.eventconduit.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Message received from the Event Conduit stream gateway.
 * Format: {"type":"event","position":42,"event":{...CloudEvent...}}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StreamMessage(
    @JsonProperty("type") String type,
    @JsonProperty("position") long position,
    @JsonProperty("event") JsonNode event
) {}
