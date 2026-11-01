"""Exercise every generated rule scenario through the actual adapter mappers."""
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator, FormatChecker
from common import mappers
from common.runtime import build_governance_event
from evaluator import adapter_event_id, native_payload, scenarios

ROOT = Path(__file__).resolve().parents[2]


class NativeIngestionTests(unittest.TestCase):
    def setUp(self):
        self.events, self.expected = scenarios(1, datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc), "test")
        self.mappers = {
            "KEYCLOAK": mappers.keycloak,
            "KUBERNETES": mappers.kubernetes_audit,
            "PIPELINE": mappers.airflow_openlineage,
            "MODEL_REGISTRY": mappers.model_registry,
            "LLM_GATEWAY": mappers.llm_gateway,
        }
        self.validator = Draft202012Validator(
            json.loads((ROOT / "schemas/governance-event.schema.json").read_text()), format_checker=FormatChecker()
        )

    def test_all_rule_scenarios_survive_http_adapter_normalization(self):
        self.assertEqual({rule for _, rule in self.expected}, {f"GOV-{i:03d}" for i in range(1, 17)})
        for event in self.events:
            with self.subTest(producer=event["producer"], event_type=event["event_type"]):
                actual = build_governance_event(native_payload(event), event["producer"], self.mappers[event["producer"]])
                self.validator.validate(actual)
                self.assertEqual(actual["event_id"], adapter_event_id(event["producer"], event["event_id"]))
                self.assertEqual(actual["event_type"], event["event_type"])
                for key, value in event["attributes"].items():
                    self.assertEqual(actual["attributes"][key], value)

    def test_llm_adapter_does_not_persist_raw_prompt_marker(self):
        event = next(event for event in self.events if event["producer"] == "LLM_GATEWAY")
        raw = native_payload(event)
        self.assertIn("E2E_RAW_PROMPT_DO_NOT_STORE", raw["prompt"])
        actual = build_governance_event(raw, "LLM_GATEWAY", mappers.llm_gateway)
        self.assertNotIn("E2E_RAW_PROMPT_DO_NOT_STORE", json.dumps(actual))

    def test_native_replay_has_stable_id_but_sources_are_distinct(self):
        for event in self.events:
            first = build_governance_event(native_payload(event), event["producer"], self.mappers[event["producer"]])
            replay = build_governance_event(native_payload(event), event["producer"], self.mappers[event["producer"]])
            self.assertEqual(first["event_id"], replay["event_id"])
        self.assertNotEqual(adapter_event_id("KEYCLOAK", "same-source-id"), adapter_event_id("KUBERNETES", "same-source-id"))


if __name__ == "__main__":
    unittest.main()
