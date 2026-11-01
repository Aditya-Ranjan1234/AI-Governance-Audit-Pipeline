package io.governance.streams;

import io.governance.model.ComplianceFinding;
import io.governance.model.GovernanceEvent;
import io.governance.rules.RuleCatalog;
import io.governance.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GovernanceTopologyTest {
    @Test void emitsOneBurstFindingOnTheFifthFailure() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        try (TopologyTestDriver driver = new TopologyTestDriver(new GovernanceTopology(RuleCatalog.load()).build(), p)) {
            JsonSerde<GovernanceEvent> eventSerde = new JsonSerde<>(GovernanceEvent.class);
            JsonSerde<ComplianceFinding> findingSerde = new JsonSerde<>(ComplianceFinding.class);
            TestInputTopic<String, GovernanceEvent> input = driver.createInputTopic(
                    "governance.keycloak.v1", Serdes.String().serializer(), eventSerde.serializer());
            TestOutputTopic<String, ComplianceFinding> output = driver.createOutputTopic(
                    GovernanceTopology.OUTPUT_TOPIC, Serdes.String().deserializer(), findingSerde.deserializer());

            Instant start = Instant.parse("2026-01-01T00:00:00Z");
            for (int i = 0; i < 5; i++) input.pipeInput("alice", failedLogin(i, start.plusSeconds(i * 20L)), start.plusSeconds(i * 20L));

            List<ComplianceFinding> findings = output.readValuesToList();
            assertEquals(1, findings.size());
            assertEquals("GOV-001", findings.getFirst().ruleId());
            assertEquals(5, findings.getFirst().frequency());
        }
    }

    @Test void promotionWithoutStateEmitsTwoFindings() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-promotion");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        try (TopologyTestDriver driver = new TopologyTestDriver(new GovernanceTopology(RuleCatalog.load()).build(), p)) {
            JsonSerde<GovernanceEvent> eventSerde = new JsonSerde<>(GovernanceEvent.class);
            JsonSerde<ComplianceFinding> findingSerde = new JsonSerde<>(ComplianceFinding.class);
            TestInputTopic<String, GovernanceEvent> input = driver.createInputTopic(
                    "governance.model-registry.v1", Serdes.String().serializer(), eventSerde.serializer());
            TestOutputTopic<String, ComplianceFinding> output = driver.createOutputTopic(
                    GovernanceTopology.OUTPUT_TOPIC, Serdes.String().deserializer(), findingSerde.deserializer());
            input.pipeInput("credit:7", modelPromotion(), Instant.parse("2026-01-01T01:00:00Z"));
            assertEquals(List.of("GOV-011", "GOV-012"), output.readValuesToList().stream().map(ComplianceFinding::ruleId).toList());
        }
    }

    private GovernanceEvent failedLogin(int index, Instant at) {
        return new GovernanceEvent("1.0", "00000000-0000-0000-0000-00000000000" + index, at, at,
                GovernanceEvent.Producer.KEYCLOAK, "LOGIN_FAILURE",
                new GovernanceEvent.Actor("alice", "USER", List.of(), null, null),
                new GovernanceEvent.Subject("IDENTITY", "alice", null, null, null, null),
                "authenticate", GovernanceEvent.Outcome.FAILURE, "prod", "burst", null, Map.of(), Map.of());
    }

    private GovernanceEvent modelPromotion() {
        Instant at = Instant.parse("2026-01-01T01:00:00Z");
        return new GovernanceEvent("1.0", "10000000-0000-0000-0000-000000000001", at, at,
                GovernanceEvent.Producer.MODEL_REGISTRY, "MODEL_PROMOTED",
                new GovernanceEvent.Actor("release-bot", "SERVICE", List.of(), null, null),
                new GovernanceEvent.Subject("MODEL", "credit:7", null, "credit", "7", null),
                "promote", GovernanceEvent.Outcome.SUCCESS, "prod", "promotion", null, Map.of(), Map.of());
    }
}

