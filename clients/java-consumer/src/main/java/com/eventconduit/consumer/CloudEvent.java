package com.eventconduit.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * CloudEvents v1.0 representation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CloudEvent(
    @JsonProperty("specversion") String specVersion,
    @JsonProperty("id") String id,
    @JsonProperty("type") String type,
    @JsonProperty("source") String source,
    @JsonProperty("subject") String subject,
    @JsonProperty("time") String time,
    @JsonProperty("datacontenttype") String dataContentType,
    @JsonProperty("data") JsonNode data
) {}
