"""Model-facing tools and strict output validation; never executable actions."""

from jsonschema import Draft202012Validator

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
                    "event_ref": {"type": "string", "maxLength": 200},
                    "event_time": {"type": "string", "maxLength": 50},
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
