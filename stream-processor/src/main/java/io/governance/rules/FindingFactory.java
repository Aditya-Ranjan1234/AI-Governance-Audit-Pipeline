package io.governance.rules;

import io.governance.model.ComplianceFinding;
import io.governance.model.GovernanceEvent;
import io.governance.model.RuleDefinition;
import io.governance.risk.BaselineRiskScorer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public final class FindingFactory {
    private FindingFactory() {}

    public static ComplianceFinding create(
            RuleDefinition rule, GovernanceEvent event, long frequency,
            Instant windowStart, Instant windowEnd, Map<String, Object> extraEvidence
    ) {
        String identity = rule.id() + ":" + event.eventId() + ":" + (windowStart == null ? "event" : windowStart);
        String findingId = UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("event_type", event.eventType());
        evidence.put("actor_id", event.actor() == null ? null : event.actor().id());
        evidence.put("subject_id", event.subject() == null ? null : event.subject().id());
        evidence.put("environment", event.environment());
        evidence.put("correlation_id", event.correlationId());
        if (extraEvidence != null) evidence.putAll(extraEvidence);
        int frequencyBand = BaselineRiskScorer.frequencyBand(frequency);
        return new ComplianceFinding(
                "1.0", findingId, rule.id(), rule.title(), event.producer(), event.eventId(), event.eventTime(),
                Instant.now(), windowStart, windowEnd, rule.severity(), BaselineRiskScorer.label(rule.severity()),
                frequency, frequencyBand, BaselineRiskScorer.score(rule.severity(), frequency),
                rule.frameworks(), evidence
        );
    }
}

