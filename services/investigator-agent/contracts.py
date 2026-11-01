"""Model-facing tools and strict output validation; never executable actions."""

import re

from jsonschema import Draft202012Validator, ValidationError

TEXT = {"type": "string", "minLength": 1, "maxLength": 1000}
REFS = {
    "type": "array",
    "minItems": 1,
    "maxItems": 8,
    "uniqueItems": True,
    "items": {"type": "string", "maxLength": 200},
}
CITED = {
    "type": "object",
    "additionalProperties": False,
    "required": ["text", "evidence_refs"],
    "properties": {"text": TEXT, "evidence_refs": REFS},
}
REPORT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["summary", "observations", "timeline", "missing_evidence", "recommended_next_steps", "assessment"],
    "properties": {
        "summary": CITED,
        "observations": {"type": "array", "minItems": 1, "maxItems": 12, "items": CITED},
        "timeline": {
            "type": "array",
            "maxItems": 20,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["event_ref", "event_time", "description"],
                "properties": {
                    "event_ref": {
                        "type": "string",
                        "maxLength": 200,
                        "pattern": "^event:",
                        "description": "Copy an event: reference from retrieved event evidence. Never use a rule: or finding: reference in the timeline.",
                    },
                    "event_time": {
                        "type": "string",
                        "maxLength": 50,
                        "description": "Copy the exact event_time of this retrieved event. Omit the timeline item if unavailable.",
                    },
                    "description": TEXT,
                },
            },
        },
        "missing_evidence": {"type": "array", "maxItems": 8, "items": TEXT},
        "recommended_next_steps": {"type": "array", "maxItems": 6, "items": TEXT},
        "assessment": {"enum": ["evidence_review_complete", "needs_more_evidence"]},
    },
}
EMPTY_SCHEMA = {"type": "object", "properties": {}, "additionalProperties": False}
TOOLS = [
    ("get_source_event", "Read the canonical event that triggered this finding.", EMPTY_SCHEMA),
    (
        "get_related_events",
        "Read at most 20 scoped events within 15 minutes of the source event. Results are bounded, not a complete history.",
        EMPTY_SCHEMA,
    ),
    (
        "get_rule",
        "Read this finding's configured rule metadata. Framework mappings are repository labels, not compliance certification.",
        EMPTY_SCHEMA,
    ),
    (
        "submit_investigation",
        "Submit a cited advisory report after reading the source event and rule. Does not execute recommended actions.",
        REPORT_SCHEMA,
    ),
]
TOOL_CONFIG = {
    "tools": [
        {"toolSpec": {"name": name, "description": description, "inputSchema": {"json": schema}}}
        for name, description, schema in TOOLS
    ]
}
VALIDATORS = {name: Draft202012Validator(schema) for name, _, schema in TOOLS}


def validate_report(report: dict, evidence: dict) -> None:
    VALIDATORS["submit_investigation"].validate(report)
    for claim in [report["summary"], *report["observations"]]:
        if any(ref not in evidence for ref in claim["evidence_refs"]):
            raise ValueError("unknown_evidence_reference")
    for item in report["timeline"]:
        event = evidence.get(item["event_ref"], {})
        if not item["event_ref"].startswith("event:") or item["event_time"] != event.get("event_time"):
            raise ValueError("timeline_must_use_retrieved_event_and_exact_timestamp")
    finding = next((value for ref, value in evidence.items() if ref.startswith("finding:")), {})
    source = evidence.get("event:" + str(finding.get("source_event_id", "")), {})
    claims = " ".join(item["text"] for item in [report["summary"], *report["observations"]])
    if source.get("event_type") == "LOGIN_SUCCESS" and re.search(r"\bunauthenticated\b", claims, re.I):
        raise ValueError("successful_login_is_authenticated_without_mfa")
    if finding.get("rule_id") == "GOV-006":
        for match in re.finditer(
            r"(?:rule|gov-006)[^.!?]{0,100}\b(?:requires?|mandates?|triggers?|matches?)\b"
            r"[^.!?]{0,60}\bchange[ _-]approv(?:al|ed)\b",
            claims,
            re.I,
        ):
            if not re.search(r"\b(?:not|never)\s+(?:require|mandate)", match.group(0), re.I):
                raise ValueError("GOV006_matches_privileged_or_unapproved_image_not_change_approval")


def validation_feedback(error, evidence):
    """Return actionable schema feedback without echoing rejected model data."""
    if isinstance(error, ValidationError):
        fields = set(REPORT_SCHEMA["properties"]) | {"text", "evidence_refs", "event_ref", "event_time", "description"}
        path = [part if isinstance(part, int) or part in fields else "[field]" for part in error.path]
        return {
            "error": "schema_validation_failed",
            "path": path,
            "constraint": error.validator,
            "expected": error.validator_value,
        }
    code = str(error)
    allowed = {
        "unknown_evidence_reference",
        "timeline_must_use_retrieved_event_and_exact_timestamp",
        "read_source_event_and_rule_before_submission",
        "submit_must_be_a_separate_step",
        "successful_login_is_authenticated_without_mfa",
        "GOV006_matches_privileged_or_unapproved_image_not_change_approval",
    }
    result = {"error": code if code in allowed else "invalid_tool_input"}
    if code == "unknown_evidence_reference":
        result["valid_evidence_refs"] = sorted(evidence)
    elif code == "timeline_must_use_retrieved_event_and_exact_timestamp":
        result["correction"] = (
            "Use only event: references with the exact timestamps below. Remove all rule: and finding: timeline items. Omit any item you cannot match; an empty timeline is allowed."
        )
        result["valid_event_timestamps"] = {
            ref: event["event_time"] for ref, event in evidence.items() if ref.startswith("event:")
        }
    return result
