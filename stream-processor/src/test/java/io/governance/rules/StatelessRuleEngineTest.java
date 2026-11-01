package io.governance.rules;

import io.governance.model.GovernanceEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatelessRuleEngineTest {
    private final StatelessRuleEngine engine = new StatelessRuleEngine(RuleCatalog.load());

    @Test void detectsPiiDisallowedModelAndInjectionInOneGatewayEvent() {
        GovernanceEvent event = event(
                GovernanceEvent.Producer.LLM_GATEWAY, "PROMPT_REQUEST",
                Map.of("pii_detected", true, "pii_handling_allowed", false,
                        "model_approved", false, "prompt_injection_score", 0.93)
        );
        assertEquals(List.of("GOV-014", "GOV-015", "GOV-016"),
                engine.evaluate(event).stream().map(f -> f.ruleId()).toList());
    }

    @Test void doesNotFlagCompliantArtifact() {
        GovernanceEvent event = event(
                GovernanceEvent.Producer.MODEL_REGISTRY, "ARTIFACT_REGISTERED",
                Map.of("signature_verified", true, "sha256", "abc123")
        );
        assertEquals(0, engine.evaluate(event).size());
    }

    @Test void catalogContainsExactlySixteenRules() {
        assertEquals(16, RuleCatalog.load().size());
    }

    private GovernanceEvent event(GovernanceEvent.Producer producer, String type, Map<String, Object> attributes) {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        return new GovernanceEvent(
                "1.0", "00000000-0000-0000-0000-000000000001", now, now, producer, type,
                new GovernanceEvent.Actor("tester", "USER", List.of(), "127.0.0.1", "team-a"),
                new GovernanceEvent.Subject("MODEL", "subject-1", "team-b", "credit", "7", "restricted"),
                "test", GovernanceEvent.Outcome.SUCCESS, "prod", "corr-1", null, attributes, Map.of()
        );
    }
}

