"""Worker dispatch and failure handling for both sides of the review conversation."""

import json
import sys

import psycopg

from agent import AgentError, BedrockModel
from review_contracts import REVIEW_TOOL_CONFIG, review_outcome
from reviewer import REVIEW_SYSTEM, REVISION_SYSTEM, ReviewTools, Reviewer, RevisionInvestigator, SnapshotTools
from store import LeaseLost

CONFIGURATION_ERRORS = {
    "bedrock_AccessDeniedException",
    "bedrock_ValidationException",
    "bedrock_ResourceNotFoundException",
    "bedrock_ServiceQuotaExceededException",
}


def workflow_model(settings, *, reviewing):
    if reviewing:
        return BedrockModel(settings, system=REVIEW_SYSTEM, tool_config=REVIEW_TOOL_CONFIG)
    return BedrockModel(settings, system=REVISION_SYSTEM)


def process_task(store, job, model):
    payload, snapshot, error = None, {}, None
    try:
        store.audit(job, {"kind": "attempt_started", "source_run_id": str(job["source_run_id"])})
        snapshot = store.snapshot(job)

        def audit(event):
            store.audit(job, event)

        if store.reviewing:
            tools = ReviewTools(snapshot)
            review = Reviewer(model, tools, store.settings, audit=audit).run()
            payload = review_outcome(review, tools.checks, revised=bool(job["revision_run_id"]))
        else:
            tools = SnapshotTools(snapshot)
            payload = RevisionInvestigator(model, tools, store.settings, audit=audit).run()
    except LeaseLost:
        return "lease_lost"
    except AgentError as failure:
        error = failure
    except (psycopg.Error, OSError):
        raise  # Leave the lease intact for recovery after a storage outage.
    except Exception:
        error = AgentError("unexpected_review_workflow_error")
    try:
        state = store.finish(job, payload=payload, evidence=snapshot.get("evidence", {}), error=error)
    except LeaseLost:
        return "lease_lost"
    print(
        json.dumps(
            {
                "agent": "reviewer" if store.reviewing else "investigator",
                "job_id": str(job["job_id"]),
                "source_run_id": str(job["source_run_id"]),
                "status": state,
            }
        ),
        file=sys.stderr,
    )
    return state
