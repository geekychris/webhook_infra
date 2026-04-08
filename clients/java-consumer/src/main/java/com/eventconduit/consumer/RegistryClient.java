package com.eventconduit.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/**
 * Fetches subscribers and subscriptions from the Event Conduit registry API.
 */
@Component
public class RegistryClient {

    private static final Logger log = LoggerFactory.getLogger(RegistryClient.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${eventconduit.registry-url:http://localhost:18081}")
    private String registryUrl;

    @Value("${eventconduit.admin-url:http://localhost:18085}")
    private String adminUrl;

    /**
     * Streaming subscription with subscriber info.
     */
    public record StreamTarget(
        String subscriptionId,
        String subscriberId,
        String subscriberName,
        List<String> eventTypes,
        String deliveryMode,
        String streamToken
    ) {
        public String displayLabel() {
            return subscriberName + " — " + String.join(", ", eventTypes)
                + " [" + subscriptionId.substring(0, Math.min(12, subscriptionId.length())) + "...]";
        }
    }

    /**
     * Fetches all streaming subscriptions with their subscriber info and stream tokens.
     */
    public List<StreamTarget> getStreamTargets() {
        try {
            // Fetch subscriptions from admin API
            var subsResp = http.send(
                HttpRequest.newBuilder(URI.create(adminUrl + "/v1/admin/subscriptions")).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            List<Map<String, Object>> subs = mapper.readValue(subsResp.body(), new TypeReference<>() {});

            // Fetch subscribers from registry
            var subrsResp = http.send(
                HttpRequest.newBuilder(URI.create(registryUrl + "/v1/subscribers")).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            List<Map<String, Object>> subscribers = mapper.readValue(subrsResp.body(), new TypeReference<>() {});

            // Build subscriber lookup
            Map<String, Map<String, Object>> subscriberMap = new HashMap<>();
            for (var s : subscribers) {
                subscriberMap.put((String) s.get("id"), s);
            }

            // Filter to streaming subscriptions and enrich
            List<StreamTarget> targets = new ArrayList<>();
            for (var sub : subs) {
                String mode = (String) sub.get("delivery_mode");
                if (!"stream".equals(mode)) continue;

                String subscriberId = (String) sub.get("subscriber_id");
                var subscriber = subscriberMap.get(subscriberId);
                String name = subscriber != null ? (String) subscriber.get("name") : "(unknown)";
                String token = subscriber != null ? (String) subscriber.getOrDefault("stream_auth", "") : "";

                @SuppressWarnings("unchecked")
                List<String> types = (List<String>) sub.getOrDefault("event_types", List.of());

                targets.add(new StreamTarget(
                    (String) sub.get("id"),
                    subscriberId,
                    name,
                    types,
                    mode,
                    token
                ));
            }

            return targets;
        } catch (Exception e) {
            log.error("Failed to fetch stream targets", e);
            return List.of();
        }
    }
}
