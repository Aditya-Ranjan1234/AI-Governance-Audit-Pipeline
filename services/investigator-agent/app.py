"""CLI and single-agent worker. Default operation: discover -> investigate -> save."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
from threading import Event
import uuid

import psycopg

from agent import AgentError, BedrockModel, Investigator
from settings import Settings
from store import EvidenceTools, LeaseLost, Store, load_catalog, serializable
from review_store import TaskStore
from review_worker import CONFIGURATION_ERRORS, process_task, workflow_model


def process_job(store, job, model, catalog, settings):
    tools, report, error = None, None, None
    try:
        store.audit(job, {"kind": "attempt_started"})
        tools = EvidenceTools(store, job["finding_id"], catalog, store.policy_hash)
        report = Investigator(model, tools, settings, audit=lambda event: store.audit(job, event)).run()
    except LeaseLost:
        return "lease_lost"
    except AgentError as failure:
        error = failure
    except (psycopg.Error, OSError):
        # Leave the lease intact when storage is unavailable; another worker can recover it.
        raise
    except Exception:
        error = AgentError("unexpected_investigation_error")
    try:
        state = store.finish(job, report=report, evidence=tools.evidence if tools else {}, error=error)
    except LeaseLost:
        return "lease_lost"
    print(
        json.dumps({"job_id": str(job["job_id"]), "finding_id": str(job["finding_id"]), "status": state}),
        file=sys.stderr,
    )
    return state


def main(argv=None):
    parser = argparse.ArgumentParser(description="One read-only Bedrock investigator")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="Apply additive schema as DB owner; does not invoke Bedrock")
    commands.add_parser("worker", help="Continuously discover and investigate persisted findings")
    commands.add_parser("once", help="Discover findings and process at most one available job")
    commands.add_parser(
        "revise-once", help="Consume at most one reviewer correction request using the original evidence"
    )
    investigate = commands.add_parser(
        "investigate", help="Investigate one persisted finding; reuse existing successful result"
    )
    investigate.add_argument("finding_id", type=uuid.UUID)
    show = commands.add_parser("show", help="Print saved reports, without calling Bedrock")
    show.add_argument("finding_id", type=uuid.UUID, nargs="?")
    commands.add_parser("status", help="Print recent job statuses, without calling Bedrock")
    args = parser.parse_args(argv)
    settings = Settings.from_env(require_model=args.command in ("worker", "once", "investigate", "revise-once"))
    if args.command == "migrate":
        with psycopg.connect(settings.dsn, autocommit=True, connect_timeout=5) as connection:
            for name in ("002_investigator.sql", "003_reviewer.sql"):
                connection.execute((Path(__file__).resolve().parents[2] / "db" / name).read_text())
        print('{"migration":"003_reviewer","status":"applied"}')
        return 0
    catalog, policy_hash = load_catalog(settings.rules_path)
    store = Store(settings, policy_hash)
    store.check_permissions()
    if args.command in ("show", "status"):
        print(json.dumps(store.reports(args.finding_id) if args.command == "show" else store.jobs(), indent=2))
        return 0
    enabled = os.getenv("INVESTIGATOR_REVISIONS_ENABLED", "false").lower()
    if enabled not in ("true", "false"):
        raise ValueError("INVESTIGATOR_REVISIONS_ENABLED must be true or false")
    revisions = (
        TaskStore(settings, reviewing=False)
        if args.command == "revise-once" or (args.command in ("worker", "once") and enabled == "true")
        else None
    )
    if revisions:
        revisions.check_permissions()
    revision_model = None
    selected = store.enqueue(args.finding_id) if args.command == "investigate" else None
    if selected and selected["status"] == "succeeded":
        print(json.dumps(store.reports(args.finding_id), indent=2))
        return 0
    model = None
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        job, state = None, "not_available"
        try:
            if revisions:
                revision_job = revisions.claim()
                if revision_job:
                    revision_model = revision_model or workflow_model(settings, reviewing=False)
                    state = process_task(revisions, revision_job, revision_model)
                    if state == "failed" and revisions.failure_code(revision_job["job_id"]) in CONFIGURATION_ERRORS:
                        print(
                            '{"error":"bedrock_configuration_error","message":"Investigator revision worker stopped."}',
                            file=sys.stderr,
                        )
                        return 2
                    if args.command != "worker":
                        return 0 if state == "succeeded" else 1
                    continue
                if args.command == "revise-once":
                    print('{"revision":"not_available"}')
                    return 0
            if selected is None:
                store.enqueue()
            job = store.claim(selected["job_id"] if selected else None)
            if job:
                model = model or BedrockModel(settings)
                state = process_job(store, job, model, catalog, settings)
                if state == "failed":
                    code = store.failure_code(job["job_id"])
                    if code in {
                        "bedrock_AccessDeniedException",
                        "bedrock_ValidationException",
                        "bedrock_ResourceNotFoundException",
                        "bedrock_ServiceQuotaExceededException",
                    }:
                        print(
                            json.dumps(
                                {
                                    "error": "bedrock_configuration_error",
                                    "code": code,
                                    "message": "Worker stopped; fix model access/configuration before resuming.",
                                }
                            ),
                            file=sys.stderr,
                        )
                        return 2
            else:
                state = "not_available"
            if args.command != "worker":
                print(json.dumps(serializable(store.reports(args.finding_id) if selected else store.jobs()), indent=2))
                return 0 if state == "succeeded" or (state == "not_available" and selected is None) else 1
        except (psycopg.Error, OSError):
            print(
                '{"error":"database_unavailable","message":"Job leases preserve recovery; no payloads logged"}',
                file=sys.stderr,
            )
            if args.command != "worker":
                return 1
        stop.wait(settings.poll_seconds if not job or state != "succeeded" else 0)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError) as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    except psycopg.Error:
        print("Database setup failed. Apply the migration and verify role membership/DSN.", file=sys.stderr)
        raise SystemExit(2) from None
