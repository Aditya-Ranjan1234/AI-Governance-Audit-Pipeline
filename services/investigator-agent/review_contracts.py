"""Cited review decisions plus narrow, independently computed evidence checks."""

from datetime import datetime
import re

from jsonschema import Draft202012Validator, ValidationError

from contracts import EMPTY_SCHEMA, REFS, TEXT

ISSUE = {
    "type": "object",
    "additionalProperties": False,
    "required": ["claim_path", "code", "explanation", "evidence_refs", "requested_correction"],
    "properties": {
        "claim_path": {"type": "string", "maxLength": 150},
        "code": {
            "enum": ["contradiction", "chronology", "unsupported_claim", "scope_overstatement", "invalid_citation"]
        },
        "explanation": TEXT,
        "evidence_refs": REFS,
        "requested_correction": TEXT,
    },
}
REVIEW_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["verdict", "summary", "issues"],
    "properties": {
        "verdict": {"enum": ["review_passed", "revision_required", "needs_human_review"]},
        "summary": TEXT,
        "issues": {"type": "array", "maxItems": 12, "items": ISSUE},
    },
}
REVIEW_TOOLS = [
    ("get_report", "Read the exact saved investigation report being reviewed.", EMPTY_SCHEMA),
    ("get_evidence", "Read only its immutable evidence snapshot and the limits of that snapshot.", EMPTY_SCHEMA),
    (
        "submit_review",
        "Submit a cited quality review. This does not approve compliance or execute actions.",
        REVIEW_SCHEMA,
    ),
]
REVIEW_VALIDATORS = {name: Draft202012Validator(schema) for name, _, schema in REVIEW_TOOLS}
REVIEW_TOOL_CONFIG = {
    "tools": [
        {"toolSpec": {"name": name, "description": description, "inputSchema": {"json": schema}}}
        for name, description, schema in REVIEW_TOOLS
    ]
}


def claims(report):
    """Only user-visible prose paths can be criticized; IDs are not editable claims."""
    result = {"/summary/text": report["summary"]["text"]}
    for n, item in enumerate(report["observations"]):
        result[f"/observations/{n}/text"] = item["text"]
    for n, item in enumerate(report["timeline"]):
        result[f"/timeline/{n}/description"] = item["description"]
    for key in ("missing_evidence", "recommended_next_steps"):
        result.update({f"/{key}/{n}": text for n, text in enumerate(report[key])})
    return result


def validate_review(review, report, evidence):
    REVIEW_VALIDATORS["submit_review"].validate(review)
    paths = claims(report)
    for issue in review["issues"]:
        if issue["claim_path"] not in paths:
            raise ValueError("unknown_claim_path")
        if any(ref not in evidence for ref in issue["evidence_refs"]):
            raise ValueError("unknown_evidence_reference")
    if review["verdict"] == "review_passed" and review["issues"]:
        raise ValueError("passing_review_cannot_contain_issues")
    if review["verdict"] == "revision_required" and not review["issues"]:
        raise ValueError("revision_requires_cited_issues")


def review_feedback(error, report, evidence):
    if isinstance(error, ValidationError):
        return {
            "error": "schema_validation_failed",
            "path": list(error.schema_path),
            "constraint": error.validator,
            "expected": error.validator_value,
        }
    code = str(error)
    allowed = {
        "unknown_claim_path",
        "unknown_evidence_reference",
        "passing_review_cannot_contain_issues",
        "revision_requires_cited_issues",
        "submit_must_be_a_separate_step",
        "read_source_event_and_rule_before_submission",
    }
    result = {"error": code if code in allowed else "invalid_tool_input"}
    if code == "unknown_claim_path":
        result["valid_claim_paths"] = list(claims(report))
    if code == "unknown_evidence_reference":
        result["valid_evidence_refs"] = sorted(evidence)
    if code == "read_source_event_and_rule_before_submission":
        result["correction"] = "Read get_report and get_evidence before submitting a review."
    return result


def deterministic_checks(report, evidence):
    """Conservative regression checks; not a general natural-language truth verifier."""
    issues = []
    finding = next((item for ref, item in evidence.items() if ref.startswith("finding:")), {})
    source_ref = "event:" + str(finding.get("source_event_id", ""))
    source = evidence.get(source_ref, {})
    cited_claims = [("/summary/text", report["summary"])] + [
        (f"/observations/{n}/text", item) for n, item in enumerate(report["observations"])
    ]
    for path, claim in cited_claims:
        text = claim["text"]
        cited = [(ref, evidence[ref]) for ref in claim["evidence_refs"] if ref in evidence and ref.startswith("event:")]
        login_refs = [
            ref
            for ref, event in cited
            if event.get("event_type") == "LOGIN_SUCCESS" and event.get("attributes", {}).get("mfa_used") is False
        ]
        if (
            login_refs
            and re.search(r"\bunauthenticated\b", text, re.I)
            and not re.search(r"\b(?:not|never)\s+(?:an?\s+)?unauthenticated\b", text, re.I)
        ):
            issues.append(
                {
                    "claim_path": path,
                    "code": "contradiction",
                    "explanation": "The cited LOGIN_SUCCESS records successful authentication without MFA.",
                    "evidence_refs": login_refs[:8],
                    "requested_correction": "Describe successful authentication without MFA, not an unauthenticated login.",
                }
            )
        if source.get("event_type") == "ROLE_GRANT" and re.search(
            r"\blogin\b.{0,120}\b(?:before|preceding|preceded)\b.{0,60}\brole[ -]grant\b", text, re.I
        ):
            for ref, event in cited:
                if event.get("event_type") != "LOGIN_SUCCESS":
                    continue
                try:
                    later = datetime.fromisoformat(event["event_time"]) > datetime.fromisoformat(source["event_time"])
                except (KeyError, ValueError, TypeError):
                    continue
                if later:
                    issues.append(
                        {
                            "claim_path": path,
                            "code": "chronology",
                            "explanation": "The cited login timestamp is later than the source role-grant timestamp.",
                            "evidence_refs": list(dict.fromkeys([source_ref, ref])),
                            "requested_correction": "Correct the prose order using the exact event timestamps. Do not infer a shared session.",
                        }
                    )
                    break
    return issues


def review_outcome(review, checks, *, revised=False):
    verdict = review["verdict"]
    if checks and verdict == "review_passed":
        verdict = "revision_required"
    limited = revised and verdict == "revision_required"
    return {
        "review": review,
        "deterministic_issues": checks,
        "effective_verdict": "needs_human_review" if limited else verdict,
        "revision_limit_reached": limited,
    }
