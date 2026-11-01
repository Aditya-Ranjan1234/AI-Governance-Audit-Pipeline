from pathlib import Path
import json
import unittest

from jsonschema import Draft202012Validator, FormatChecker

from common.mappers import airflow_openlineage, keycloak, kubernetes_audit, llm_gateway, model_registry
from common.runtime import build_governance_event


SCHEMA = json.loads(Path("/app/governance-event.schema.json").read_text())
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())


class MapperTests(unittest.TestCase):
    def assert_event(self, producer, mapper, raw, expected_type):
        event = build_governance_event(raw, producer, mapper)
        VALIDATOR.validate(event)
        self.assertEqual(expected_type, event["event_type"])
        return event

    def test_keycloak(self):
        event = self.assert_event("KEYCLOAK", keycloak, {
            "id": "kc-1", "time": 1767225600000, "type": "LOGIN_ERROR", "userId": "u-1",
            "details": {"privileged_identity": True, "mfa_used": False},
        }, "LOGIN_FAILURE")
        self.assertEqual("u-1", event["actor"]["id"])

    def test_kubernetes_audit(self):
        event = self.assert_event("KUBERNETES", kubernetes_audit, {
            "auditID": "ka-1", "requestReceivedTimestamp": "2026-01-01T00:00:00Z", "verb": "create",
            "user": {"username": "admin", "groups": ["system:masters"]},
            "objectRef": {"resource": "clusterrolebindings", "name": "root"},
            "requestObject": {"roleRef": {"name": "cluster-admin"}},
        }, "CLUSTER_ROLE_BINDING")
        self.assertEqual("cluster-admin", event["attributes"]["cluster_role"])

    def test_airflow_openlineage(self):
        event = self.assert_event("PIPELINE", airflow_openlineage, {
            "eventTime": "2026-01-01T00:00:00Z", "eventType": "COMPLETE",
            "run": {"runId": "run-1", "facets": {"governance": {"event_type": "DRIFT_EVALUATION", "metric_value": 0.4, "threshold": 0.2}}},
            "job": {"namespace": "airflow", "name": "drift-check"},
        }, "DRIFT_EVALUATION")
        self.assertEqual(0.4, event["attributes"]["metric_value"])

    def test_model_registry(self):
        event = self.assert_event("MODEL_REGISTRY", model_registry, {
            "id": "mr-1", "event_time": "2026-01-01T00:00:00Z", "action": "promoted",
            "model": {"name": "credit-risk", "version": "7"}, "environment": "prod",
        }, "MODEL_PROMOTED")
        self.assertEqual("credit-risk:7", event["subject"]["id"])

    def test_llm_gateway_drops_raw_prompt(self):
        event = self.assert_event("LLM_GATEWAY", llm_gateway, {
            "request_id": "llm-1", "event_time": "2026-01-01T00:00:00Z", "user_id": "u-1",
            "prompt": "secret raw prompt", "pii_detected": True, "pii_handling_allowed": False,
            "model": {"name": "assistant", "version": "1"},
        }, "PROMPT_REQUEST")
        self.assertNotIn("prompt", event["attributes"])


if __name__ == "__main__":
    unittest.main()
