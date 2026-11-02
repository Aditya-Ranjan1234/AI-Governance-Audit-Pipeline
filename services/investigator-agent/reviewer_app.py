"""Second-agent CLI: discover reports -> review -> issue a bounded revision request."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
from threading import Event
import uuid

import psycopg

from review_store import TaskStore
from review_worker import CONFIGURATION_ERRORS, process_task, workflow_model
from settings import Settings


def main(argv=None):
    parser = argparse.ArgumentParser(description="Evidence reviewer agent")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("worker", "once", "migrate", "status"):
        commands.add_parser(command)
    commands.add_parser("review").add_argument("source_run_id", type=uuid.UUID)
    commands.add_parser("show").add_argument("source_run_id", type=uuid.UUID, nargs="?")
    args = parser.parse_args(argv)
    settings = Settings.from_env(
        require_model=args.command in ("worker", "once", "review"), prefix="REVIEWER", model_env="REVIEWER_MODEL_ID"
    )
    if args.command == "migrate":
        with psycopg.connect(settings.dsn, autocommit=True, connect_timeout=5) as c:
            root = Path(__file__).resolve().parents[2]
            for file in ("002_investigator.sql", "003_reviewer.sql"):
                c.execute((root / "db" / file).read_text())
        print('{"migration":"003_reviewer","status":"applied"}')
        return 0
    store = TaskStore(settings, reviewing=True, revision_model_id=os.getenv("BEDROCK_MODEL_ID") or settings.model_id)
    store.check_permissions()
    selected = getattr(args, "source_run_id", None)
    if args.command in ("show", "status"):
        print(json.dumps(store.results(selected) if args.command == "show" else store.jobs(), indent=2))
        return 0
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    model = None
    while not stop.is_set():
        job = None
        try:
            store.enqueue(selected)
            job = store.claim(selected)
            state = "not_available"
            if job:
                model = model or workflow_model(settings, reviewing=True)
                state = process_task(store, job, model)
                if state == "failed" and store.failure_code(job["job_id"]) in CONFIGURATION_ERRORS:
                    print(
                        '{"error":"bedrock_configuration_error","message":"Reviewer stopped; fix model access/configuration."}',
                        file=sys.stderr,
                    )
                    return 2
            if args.command != "worker":
                print(json.dumps(store.results(selected), indent=2))
                if job:
                    return 0 if state == "succeeded" else 1
                # No work is not success if a selected run has an unresolved/failed job.
                return 0 if selected is None or store.selected_complete(selected) else 1
        except (psycopg.Error, OSError):
            print(
                '{"error":"database_unavailable","message":"Leases preserve recovery; no payloads logged."}',
                file=sys.stderr,
            )
            if args.command != "worker":
                return 1
        stop.wait(settings.poll_seconds if not job else 0)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError) as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    except psycopg.Error:
        print("Reviewer database setup failed; apply migration 003 and verify role membership.", file=sys.stderr)
        raise SystemExit(2) from None
