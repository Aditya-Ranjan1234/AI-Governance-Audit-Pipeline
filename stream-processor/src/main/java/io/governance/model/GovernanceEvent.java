package io.governance.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = false)
public record GovernanceEvent(
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("event_id") String eventId,
        @JsonProperty("event_time") Instant eventTime,
        @JsonProperty("ingested_at") Instant ingestedAt,
        Producer producer,
        @JsonProperty("event_type") String eventType,
        Actor actor,
        Subject subject,
        String action,
        Outcome outcome,
        String environment,
        @JsonProperty("correlation_id") String correlationId,
        @JsonProperty("trace_id") String traceId,
        Map<String, Object> attributes,
        Map<String, String> labels
) {
    public enum Producer { KEYCLOAK, KUBERNETES, PIPELINE, MODEL_REGISTRY, LLM_GATEWAY }
    public enum Outcome { SUCCESS, FAILURE, UNKNOWN }

    public record Actor(String id, String type, List<String> roles, String ip, String namespace) {
        public Actor {
            roles = roles == null ? List.of() : List.copyOf(roles);
        }
    }

    public record Subject(
            String type,
            String id,
            String namespace,
            @JsonProperty("model_name") String modelName,
            @JsonProperty("model_version") String modelVersion,
            @JsonProperty("data_classification") String dataClassification
    ) {}

    public GovernanceEvent {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        labels = labels == null ? Map.of() : Map.copyOf(labels);
    }

    public String modelKey() {
        if (subject == null || subject.modelName() == null || subject.modelVersion() == null) return null;
        return subject.modelName() + ":" + subject.modelVersion();
    }
}

