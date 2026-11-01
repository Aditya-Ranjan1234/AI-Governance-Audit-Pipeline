package io.governance.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record ComplianceFinding(
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("finding_id") String findingId,
        @JsonProperty("rule_id") String ruleId,
        String title,
        GovernanceEvent.Producer producer,
        @JsonProperty("source_event_id") String sourceEventId,
        @JsonProperty("source_event_time") Instant sourceEventTime,
        @JsonProperty("detected_at") Instant detectedAt,
        @JsonProperty("window_start") Instant windowStart,
        @JsonProperty("window_end") Instant windowEnd,
        int severity,
        @JsonProperty("severity_label") String severityLabel,
        long frequency,
        @JsonProperty("frequency_band") int frequencyBand,
        @JsonProperty("baseline_risk") double baselineRisk,
        Map<String, List<String>> frameworks,
        Map<String, Object> evidence
) {}

