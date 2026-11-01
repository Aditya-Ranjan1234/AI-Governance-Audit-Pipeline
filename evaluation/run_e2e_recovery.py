"""Exercise recovery only in an explicitly named, isolated local E2E Compose project."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time

import psycopg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("fault", choices=["processor", "broker"])
    parser.add_argument("--project", required=True)
    parser.add_argument("--dsn", required=True, help="Local test database owner DSN")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.project.startswith("philstock-e2e-"):
        parser.error("Recovery tests require a separate philstock-e2e-* Compose project")
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(__file__).resolve().parents[1]
    compose = ["docker", "compose", "-p", args.project, "-f", "docker-compose.yml", "-f", "evaluation/compose.e2e.yml"]
    evaluator = compose + ["--profile", "evaluation", "run", "--rm", "--no-deps", "evaluator"]
    started = datetime.now(timezone.utc)
    with (args.output / "injection.json").open("w") as output, (args.output / "commands.log").open("w") as log:
        process = subprocess.Popen(evaluator + ["inject", "--repeats", "1", "--via-adapters", "--delay-ms", "250"],
                                   cwd=root, stdout=output, stderr=log)
        try:
            with psycopg.connect(args.dsn, autocommit=True) as connection:
                deadline = time.monotonic() + 30
                active = None
                while time.monotonic() < deadline:
                    active = connection.execute("""SELECT e.run_id,count(r.event_id) FROM evaluation_runs e
                        JOIN evaluation_events v USING(run_id) LEFT JOIN raw_events r ON r.event_id=v.event_id
                        WHERE e.started_at >= %s GROUP BY e.run_id HAVING count(r.event_id) BETWEEN 2 AND 23""",
                                                (started,)).fetchone()
                    if active:
                        break
                    if process.poll() is not None:
                        raise RuntimeError("Injection ended before an in-flight fault could be applied")
                    time.sleep(0.1)
                if not active:
                    raise RuntimeError("Could not observe in-flight events before timeout")
            fault_start = time.monotonic()
            command = (["up", "-d", "--no-deps", "--no-build", "--pull", "never", "--force-recreate", "stream-processor"]
                       if args.fault == "processor" else ["restart", "redpanda"])
            subprocess.run(compose + command, cwd=root, stdout=log, stderr=log, check=True, timeout=90)
            fault_seconds = round(time.monotonic() - fault_start, 3)
            if process.wait(timeout=90) != 0:
                raise RuntimeError("Injection failed; see commands.log")
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
    injection = json.loads((args.output / "injection.json").read_text())
    with (args.output / "metrics.json").open("w") as output, (args.output / "commands.log").open("a") as log:
        subprocess.run(evaluator + ["report", "--wait", "90", "--run-id", injection["run_id"]],
                       cwd=root, stdout=output, stderr=log, check=True, timeout=110)
    result = {"fault": args.fault, "run_id": injection["run_id"], "events_persisted_before_fault": active[1],
              "fault_command_seconds": fault_seconds, "metrics": json.loads((args.output / "metrics.json").read_text())}
    (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
