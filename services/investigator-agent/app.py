"""CLI and single-agent worker. Default operation: discover -> investigate -> save."""

import argparse
import json
from pathlib import Path
import signal
import sys
from threading import Event
import uuid

import psycopg

from agent import AgentError, BedrockModel, Investigator
from settings import Settings
from store import EvidenceTools, LeaseLost, Store, load_catalog, serializable


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
    investigate = commands.add_parser(
        "investigate", help="Investigate one persisted finding; reuse existing successful result"
    )
    investigate.add_argument("finding_id", type=uuid.UUID)
    show = commands.add_parser("show", help="Print saved reports, without calling Bedrock")
    show.add_argument("finding_id", type=uuid.UUID, nargs="?")
    commands.add_parser("status", help="Print recent job statuses, without calling Bedrock")
    args = parser.parse_args(argv)
    settings = Settings.from_env(require_model=args.command in ("worker", "once", "investigate"))
    if args.command == "migrate":
        sql = Path(__file__).resolve().parents[2] / "db/002_investigator.sql"
        with psycopg.connect(settings.dsn, autocommit=True, connect_timeout=5) as connection:
            connection.execute(sql.read_text())
        print('{"migration":"002_investigator","status":"applied"}')
        return 0
    catalog, policy_hash = load_catalog(settings.rules_path)
    store = Store(settings, policy_hash)
    store.check_permissions()
    if args.command in ("show", "status"):
        print(json.dumps(store.reports(args.finding_id) if args.command == "show" else store.jobs(), indent=2))
        return 0
    selected = store.enqueue(args.finding_id) if args.command == "investigate" else None
    if selected and selected["status"] == "succeeded":
        print(json.dumps(store.reports(args.finding_id), indent=2))
        return 0
    model = BedrockModel(settings)
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        job, state = None, "not_available"
        try:
            if selected is None:
                store.enqueue()
            job = store.claim(selected["job_id"] if selected else None)
            if job:
                state = process_job(store, job, model, catalog, settings)
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
