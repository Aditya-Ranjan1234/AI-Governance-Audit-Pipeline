"""Durable, role-separated agent communication. Models cannot issue database queries."""

from contextlib import contextmanager
import uuid

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agent import AgentError
from store import LeaseLost, serializable

REVIEWER_VERSION = "evidence-reviewer-v1.1"
REVISION_VERSION = "investigator-revision-v1.1"


class TaskStore:
    def __init__(self, settings, *, reviewing, revision_model_id=None):
        self.settings, self.reviewing = settings, reviewing
        self.prefix = "reviewer." if reviewing else "investigator.revision_"
        self.role = "governance_reviewer" if reviewing else "governance_investigator"
        self.version = REVIEWER_VERSION if reviewing else REVISION_VERSION
        self.revision_model_id = revision_model_id or settings.model_id

    @contextmanager
    def connection(self):
        with psycopg.connect(self.settings.dsn, row_factory=dict_row, connect_timeout=5) as c:
            # Identifiers here are fixed program constants, never model/user query input.
            c.execute(f"SET LOCAL ROLE {self.role}")
            c.execute("SET LOCAL statement_timeout='5s'")
            yield c

    def check_permissions(self):
        with self.connection() as c:
            tables = ["public.raw_events", "public.compliance_findings", "investigator.runs"]
            if self.reviewing:
                tables.append("investigator.revision_runs")
            for table in tables:
                for permission in ("UPDATE", "DELETE", "TRUNCATE"):
                    if c.execute(
                        "SELECT has_table_privilege(current_user,%s,%s) AS allowed", (table, permission)
                    ).fetchone()["allowed"]:
                        raise ValueError("Agent role must not mutate source evidence or saved reports")
            for table in ("public.raw_events", "public.compliance_findings"):
                if c.execute("SELECT has_table_privilege(current_user,%s,'INSERT') AS allowed", (table,)).fetchone()[
                    "allowed"
                ]:
                    raise ValueError("Agent role must not insert domain events or findings")
            c.execute(f"SELECT job_id FROM {self.prefix}jobs LIMIT 0")

    def enqueue(self, source_run_id=None):
        if not self.reviewing:
            raise ValueError("Only the reviewer discovers review work")
        with self.connection() as c:
            if (
                source_run_id
                and not c.execute(
                    """SELECT 1 FROM investigator.runs r JOIN investigator.jobs j USING(job_id)
                WHERE r.run_id=%s AND r.status='succeeded'
                  AND r.evidence->('finding:'||j.finding_id::text)->>'rule_id'=ANY(%s)""",
                    (source_run_id, list(self.settings.rules)),
                ).fetchone()
            ):
                raise ValueError("Successful source run not found or outside REVIEWER_RULE_IDS")
            originals = c.execute(
                """
                WITH latest AS (
                  SELECT DISTINCT ON (j.finding_id) r.run_id FROM investigator.runs r
                  JOIN investigator.jobs j USING(job_id)
                  WHERE r.status='succeeded' AND (%s::uuid IS NULL OR r.run_id=%s::uuid)
                    AND r.evidence->('finding:'||j.finding_id::text)->>'rule_id'=ANY(%s)
                  ORDER BY j.finding_id,r.finished_at DESC,r.run_id
                ) SELECT run_id FROM latest WHERE NOT EXISTS (
                  SELECT 1 FROM reviewer.jobs q WHERE q.source_run_id=latest.run_id AND q.revision_run_id IS NULL
                    AND q.worker_version=%s AND q.model_id=%s AND q.revision_model_id=%s
                ) ORDER BY run_id LIMIT 100
                """,
                (
                    source_run_id,
                    source_run_id,
                    list(self.settings.rules),
                    self.version,
                    self.settings.model_id,
                    self.revision_model_id,
                ),
            ).fetchall()
            created = 0
            for row in originals:
                created += c.execute(
                    """INSERT INTO reviewer.jobs
                    (job_id,source_run_id,worker_version,model_id,revision_model_id)
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (uuid.uuid4(), row["run_id"], self.version, self.settings.model_id, self.revision_model_id),
                ).rowcount
            # A successful correction is the durable return message to the second agent.
            revisions = c.execute(
                """SELECT r.run_id,q.source_run_id FROM investigator.revision_runs r
                JOIN investigator.revision_jobs q USING(job_id)
                JOIN reviewer.runs feedback ON feedback.run_id=q.review_run_id
                JOIN reviewer.jobs original ON original.job_id=feedback.job_id
                WHERE r.status='succeeded' AND original.worker_version=%s AND original.model_id=%s
                  AND original.revision_model_id=%s
                  AND (%s::uuid IS NULL OR q.source_run_id=%s::uuid)
                  AND NOT EXISTS(SELECT 1 FROM reviewer.jobs target WHERE target.revision_run_id=r.run_id
                    AND target.worker_version=original.worker_version AND target.model_id=original.model_id
                    AND target.revision_model_id=original.revision_model_id)
                ORDER BY r.finished_at,r.run_id LIMIT 100""",
                (self.version, self.settings.model_id, self.revision_model_id, source_run_id, source_run_id),
            ).fetchall()
            for row in revisions:
                created += c.execute(
                    """INSERT INTO reviewer.jobs
                    (job_id,source_run_id,revision_run_id,worker_version,model_id,revision_model_id)
                    VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (
                        uuid.uuid4(),
                        row["source_run_id"],
                        row["run_id"],
                        self.version,
                        self.settings.model_id,
                        self.revision_model_id,
                    ),
                ).rowcount
            return created

    def claim(self, source_run_id=None):
        with self.connection() as c:
            config_sql = "AND revision_model_id=%s" if self.reviewing else ""
            config_args = [self.revision_model_id] if self.reviewing else []
            c.execute(
                f"""UPDATE {self.prefix}jobs SET status='failed',error_code='lease_expired',
                lease_token=NULL,lease_until=NULL,updated_at=now()
                WHERE status='running' AND lease_until<now() AND attempts>=%s
                  AND worker_version=%s AND model_id=%s {config_sql}""",
                [self.settings.max_attempts, self.version, self.settings.model_id, *config_args],
            )
            return c.execute(
                f"""WITH candidate AS (
                SELECT q.job_id FROM {self.prefix}jobs q WHERE worker_version=%s AND model_id=%s {config_sql}
                  AND (%s::uuid IS NULL OR q.source_run_id=%s::uuid) AND attempts<%s
                  AND EXISTS(SELECT 1 FROM investigator.runs original JOIN investigator.jobs origin USING(job_id)
                    WHERE original.run_id=q.source_run_id
                      AND original.evidence->('finding:'||origin.finding_id::text)->>'rule_id'=ANY(%s))
                  AND ((status IN ('pending','retry') AND available_at<=now()) OR (status='running' AND lease_until<now()))
                ORDER BY created_at,job_id FOR UPDATE OF q SKIP LOCKED LIMIT 1
                ) UPDATE {self.prefix}jobs q SET status='running',attempts=attempts+1,
                    lease_token=%s,lease_until=now()+(%s*interval '1 second'),updated_at=now()
                FROM candidate WHERE q.job_id=candidate.job_id RETURNING q.*""",
                [
                    self.version,
                    self.settings.model_id,
                    *config_args,
                    source_run_id,
                    source_run_id,
                    self.settings.max_attempts,
                    list(self.settings.rules),
                    uuid.uuid4(),
                    self.settings.timeout_seconds + 150,
                ],
            ).fetchone()

    def snapshot(self, job):
        with self.connection() as c:
            original = c.execute(
                "SELECT report,evidence FROM investigator.runs WHERE run_id=%s AND status='succeeded'",
                (job["source_run_id"],),
            ).fetchone()
            if original is None:
                raise AgentError("snapshot_unavailable")
            result = dict(original, source_run_id=job["source_run_id"], revision_run_id=job.get("revision_run_id"))
            if self.reviewing and job["revision_run_id"]:
                revision = c.execute(
                    "SELECT payload FROM investigator.revision_runs WHERE run_id=%s AND status='succeeded'",
                    (job["revision_run_id"],),
                ).fetchone()
                if revision is None:
                    raise AgentError("revision_snapshot_unavailable")
                result["report"] = revision["payload"]
            if not self.reviewing:
                result["feedback"] = c.execute(
                    "SELECT payload FROM reviewer.runs WHERE run_id=%s AND status='succeeded'", (job["review_run_id"],)
                ).fetchone()["payload"]
            return serializable(result)

    def audit(self, job, event):
        with self.connection() as c:
            locked = c.execute(
                f"""SELECT job_id FROM {self.prefix}jobs WHERE job_id=%s AND status='running'
                AND lease_token=%s AND lease_until>now() FOR UPDATE""",
                (job["job_id"], job["lease_token"]),
            ).fetchone()
            if locked is None:
                raise LeaseLost()
            self._audit(c, job, event)

    def _audit(self, c, job, event):
        c.execute(
            f"INSERT INTO {self.prefix}audit_events(audit_id,job_id,attempt,kind,details) VALUES(%s,%s,%s,%s,%s)",
            (uuid.uuid4(), job["job_id"], job["attempts"], event["kind"], Jsonb(event)),
        )

    def finish(self, job, payload=None, evidence=None, error=None):
        state = (
            "succeeded"
            if error is None
            else "retry"
            if error.retryable and job["attempts"] < self.settings.max_attempts
            else "failed"
        )
        run_id = uuid.uuid4()
        base_delay = 60 if error and error.code == "bedrock_ThrottlingException" else 5
        with self.connection() as c:
            locked = c.execute(
                f"""UPDATE {self.prefix}jobs SET status=%s,error_code=%s,
                available_at=now()+(%s*interval '1 second'),lease_token=NULL,lease_until=NULL,updated_at=now()
                WHERE job_id=%s AND status='running' AND lease_token=%s AND lease_until>now() RETURNING job_id""",
                (
                    state,
                    error.code if error else None,
                    min(300, base_delay * 2 ** (job["attempts"] - 1)),
                    job["job_id"],
                    job["lease_token"],
                ),
            ).fetchone()
            if locked is None:
                raise LeaseLost()
            c.execute(
                f"""INSERT INTO {self.prefix}runs(run_id,job_id,attempt,status,payload,evidence,error_code)
                VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                (
                    run_id,
                    job["job_id"],
                    job["attempts"],
                    state,
                    Jsonb(payload) if payload else None,
                    Jsonb(evidence or {}),
                    error.code if error else None,
                ),
            )
            event = {
                "kind": "task_finished",
                "status": state,
                "run_id": str(run_id),
                "source_run_id": str(job["source_run_id"]),
            }
            if self.reviewing and state == "succeeded":
                event["verdict"] = payload["effective_verdict"]
                if payload["effective_verdict"] == "revision_required" and job["revision_run_id"] is None:
                    request_id = uuid.uuid4()
                    c.execute(
                        """INSERT INTO investigator.revision_jobs
                        (job_id,source_run_id,review_run_id,worker_version,model_id) VALUES(%s,%s,%s,%s,%s)""",
                        (request_id, job["source_run_id"], run_id, REVISION_VERSION, job["revision_model_id"]),
                    )
                    event["revision_request_id"] = str(request_id)
            self._audit(c, job, event)
        return state

    def failure_code(self, job_id):
        with self.connection() as c:
            return c.execute(f"SELECT error_code FROM {self.prefix}jobs WHERE job_id=%s", (job_id,)).fetchone()[
                "error_code"
            ]

    def selected_complete(self, source_run_id):
        with self.connection() as c:
            row = c.execute(
                """SELECT status FROM reviewer.jobs WHERE source_run_id=%s
                AND worker_version=%s AND model_id=%s AND revision_model_id=%s
                ORDER BY (revision_run_id IS NOT NULL) DESC,created_at DESC LIMIT 1""",
                (source_run_id, self.version, self.settings.model_id, self.revision_model_id),
            ).fetchone()
            return row is not None and row["status"] == "succeeded"

    def jobs(self):
        with self.connection() as c:
            return serializable(
                c.execute(f"SELECT * FROM {self.prefix}jobs ORDER BY updated_at DESC LIMIT 100").fetchall()
            )

    def results(self, source_run_id=None):
        with self.connection() as c:
            return serializable(
                c.execute(
                    """SELECT * FROM reviewer.latest_results
                WHERE (%s::uuid IS NULL OR source_run_id=%s::uuid) ORDER BY finished_at DESC LIMIT 100""",
                    (source_run_id, source_run_id),
                ).fetchall()
            )
