from copy import deepcopy
from pathlib import Path

import pytest

from settings import Settings

ROOT = Path(__file__).resolve().parents[3]
FINDING_ID = "11111111-1111-4111-8111-111111111111"
EVENT_ID = "22222222-2222-4222-8222-222222222222"
EVENT_TIME = "2026-11-01T09:00:00Z"


@pytest.fixture
def settings():
    return Settings("postgresql://unused", "test-model", "us-west-2", ROOT / "config/rules.yml")


def tool_response(name, arguments=None, call_id="call-1"):
    return {
        "output": {
            "message": {
                "role": "assistant",
                "content": [{"toolUse": {"toolUseId": call_id, "name": name, "input": arguments or {}}}],
            }
        },
        "stopReason": "tool_use",
        "usage": {"inputTokens": 10, "outputTokens": 20, "totalTokens": 30},
        "metrics": {"latencyMs": 1},
    }


def report():
    return {
        "summary": {
            "text": "A production promotion was flagged for missing approval evidence.",
            "evidence_refs": ["finding:" + FINDING_ID],
        },
        "observations": [
            {"text": "The source records a production model promotion.", "evidence_refs": ["event:" + EVENT_ID]}
        ],
        "timeline": [{"event_ref": "event:" + EVENT_ID, "event_time": EVENT_TIME, "description": "Model promoted."}],
        "missing_evidence": ["The bounded event sample does not establish whether an approval exists elsewhere."],
        "recommended_next_steps": ["Ask the model owner to provide the approval record."],
        "assessment": "needs_more_evidence",
    }


class ScriptedModel:
    def __init__(self, replies):
        self.replies, self.requests = iter(replies), []

    def converse(self, messages):
        self.requests.append(deepcopy(messages))
        response = next(self.replies)
        if isinstance(response, Exception):
            raise response
        return deepcopy(response)


class MemoryTools:
    def __init__(self):
        self.finding_ref = "finding:" + FINDING_ID
        self.finding = {"finding_id": FINDING_ID, "rule_id": "GOV-011"}
        self.evidence = {self.finding_ref: self.finding}
        self.calls = []

    def call(self, name):
        self.calls.append(name)
        if name == "get_source_event":
            self.evidence["event:" + EVENT_ID] = {"event_time": EVENT_TIME}
        if name == "get_rule":
            self.evidence["rule:GOV-011:hash"] = {"id": "GOV-011"}
        return {"retrieved": name}


def successful_model(result=None):
    return ScriptedModel(
        [
            tool_response("get_source_event"),
            tool_response("get_rule"),
            tool_response("submit_investigation", result or report()),
        ]
    )
