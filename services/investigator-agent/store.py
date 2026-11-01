"""Fixed, parameterized SQL. Domain tables are read-only under a restricted role."""

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import uuid

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
import yaml

from agent import AgentError
from settings import AGENT_VERSION


class LeaseLost(Exception):
    pass


def serializable(value):
    def convert(v):
        if isinstance(v, datetime):
            return v.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if isinstance(v, Decimal):
            return float(v)
        return str(v)

    return json.loads(json.dumps(value, default=convert, allow_nan=False))


def load_catalog(path: Path):
    raw = path.read_bytes()
    data = yaml.safe_load(raw)
    rules = {row["id"]: row for row in data["rules"]}
    if set(rules) != {f"GOV-{n:03d}" for n in range(1, 17)}:
        raise ValueError("Rule catalog must contain GOV-001..GOV-016")
    return rules, hashlib.sha256(raw).hexdigest()


class Store:
    def __init__(self, settings, policy_hash):
        self.settings, self.policy_hash = settings, policy_hash

    @contextmanager
    def connection(self):
        with psycopg.connect(self.settings.dsn, row_factory=dict_row, connect_timeout=5) as conn:
            conn.execute("SET LOCAL ROLE governance_investigator")
            conn.execute("SET LOCAL statement_timeout = '5s'")
            yield conn

    def check_permissions(self):
        with self.connection() as conn:
            for table in ("public.raw_events", "public.compliance_findings"):
                for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                    if conn.execute(
                        "SELECT has_table_privilege(current_user, %s, %s) AS allowed", (table, privilege)
                    ).fetchone()["allowed"]:
                        raise ValueError("Investigator role must not modify source tables")
            conn.execute("SELECT job_id FROM investigator.jobs LIMIT 0")

    def enqueue(self, finding_id=None):
        """Discover only persisted findings; UNIQUE identity makes polling/replay idempotent."""
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT f.finding_id FROM public.compliance_findings f
                WHERE f.rule_id = ANY(%s)
                  AND (%s::uuid IS NULL OR f.finding_id=%s::uuid)
                  AND NOT EXISTS (
                    SELECT 1 FROM investigator.jobs j WHERE j.finding_id=f.finding_id
                    AND j.agent_version=%s AND j.policy_hash=%s AND j.model_id=%s)
                ORDER BY f.detected_at, f.finding_id LIMIT 100
                """,
                (
                    list(self.settings.rules),
                    finding_id,
                    finding_id,
                    AGENT_VERSION,
                    self.policy_hash,
                    self.settings.model_id,
                ),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """INSERT INTO investigator.jobs
                    (job_id,finding_id,agent_version,policy_hash,model_id)
                    VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (uuid.uuid4(), row["finding_id"], AGENT_VERSION, self.policy_hash, self.settings.model_id),
                )
            if finding_id:
                job = conn.execute(
                    """SELECT * FROM investigator.jobs WHERE finding_id=%s
                    AND agent_version=%s AND policy_hash=%s AND model_id=%s""",
                    (finding_id, AGENT_VERSION, self.policy_hash, self.settings.model_id),
                ).fetchone()
                if job is None:
                    raise ValueError("Finding not found or rule is outside INVESTIGATOR_RULE_IDS")
                return job
            return len(rows)

    def claim(self, job_id=None):
        token = uuid.uuid4()
        lease_seconds = self.settings.timeout_seconds + 150
        with self.connection() as conn:
            # An abandoned final attempt becomes terminal instead of remaining 'running' forever.
            conn.execute(
                """UPDATE investigator.jobs SET status='failed', error_code='lease_expired',
                lease_token=NULL, lease_until=NULL, updated_at=now()
                WHERE status='running' AND lease_until<now() AND attempts >= %s
                  AND agent_version=%s AND policy_hash=%s AND model_id=%s""",
                (self.settings.max_attempts, AGENT_VERSION, self.policy_hash, self.settings.model_id),
            )
            return conn.execute(
                """
                WITH candidate AS (
                  SELECT job_id FROM investigator.jobs
                  WHERE agent_version=%s AND policy_hash=%s AND model_id=%s
                    AND EXISTS (SELECT 1 FROM public.compliance_findings f
                                WHERE f.finding_id=jobs.finding_id AND f.rule_id=ANY(%s))
                    AND (%s::uuid IS NULL OR job_id=%s::uuid) AND attempts < %s
                    AND ((status IN ('pending','retry') AND available_at<=now())
                         OR (status='running' AND lease_until<now()))
                  ORDER BY created_at, job_id FOR UPDATE SKIP LOCKED LIMIT 1
                ) UPDATE investigator.jobs j SET status='running', attempts=attempts+1,
                  lease_token=%s, lease_until=now()+(%s * interval '1 second'), updated_at=now()
                  FROM candidate c WHERE j.job_id=c.job_id RETURNING j.*
                """,
                (
                    AGENT_VERSION,
                    self.policy_hash,
                    self.settings.model_id,
                    list(self.settings.rules),
                    job_id,
                    job_id,
                    self.settings.max_attempts,
                    token,
                    lease_seconds,
                ),
            ).fetchone()

    def audit(self, job, event):
        with self.connection() as conn:
            active = conn.execute(
                """SELECT job_id FROM investigator.jobs WHERE job_id=%s
                AND lease_token=%s AND status='running' AND lease_until>now() FOR UPDATE""",
                (job["job_id"], job["lease_token"]),
            ).fetchone()
            if active is None:
                raise LeaseLost()
            conn.execute(
                """INSERT INTO investigator.audit_events
                (audit_id,job_id,attempt,kind,details) VALUES (%s,%s,%s,%s,%s)""",
                (uuid.uuid4(), job["job_id"], job["attempts"], event["kind"], Jsonb(event)),
            )

    def finish(self, job, report=None, evidence=None, error=None):
        retry = error is not None and error.retryable and job["attempts"] < self.settings.max_attempts
        state = "succeeded" if error is None else ("retry" if retry else "failed")
        delay = min(300, 5 * 2 ** (job["attempts"] - 1))
        with self.connection() as conn:
            updated = conn.execute(
                """UPDATE investigator.jobs SET status=%s, error_code=%s,
                available_at=now()+(%s * interval '1 second'), lease_token=NULL, lease_until=NULL, updated_at=now()
                WHERE job_id=%s AND lease_token=%s AND status='running' AND lease_until>now() RETURNING job_id""",
                (state, error.code if error else None, delay, job["job_id"], job["lease_token"]),
            ).fetchone()
            if updated is None:
                raise LeaseLost()
            conn.execute(
                """INSERT INTO investigator.runs
                (run_id,job_id,attempt,status,report,evidence,error_code) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (
                    uuid.uuid4(),
                    job["job_id"],
                    job["attempts"],
                    state,
                    Jsonb(report) if report is not None else None,
                    Jsonb(evidence or {}),
                    error.code if error else None,
                ),
            )
        return state

    def finding(self, finding_id):
        with self.connection() as conn:
            row = conn.execute(
                """SELECT finding_id,rule_id,title,producer,source_event_id,
                source_event_time,detected_at,severity_label,baseline_risk,recommended_risk
                FROM public.compliance_findings WHERE finding_id=%s""",
                (finding_id,),
            ).fetchone()
        if row is None:
            raise AgentError("finding_unavailable", retryable=True)
        return serializable(row)

    def source(self, event_id):
        with self.connection() as conn:
            row = conn.execute("SELECT payload FROM public.raw_events WHERE event_id=%s", (event_id,)).fetchone()
        if row is None:
            raise AgentError("source_event_unavailable", retryable=True)
        return row["payload"]

    def related(self, source):
        subject, actor = source.get("subject", {}), source.get("actor", {})
        model = subject.get("model_name")
        version = subject.get("model_version")
        if not model or not version or model == "unknown-model" or version == "unknown-version":
            model, version = None, None
        actor_id = actor.get("id")
        if actor_id in (None, "", "unknown", "airflow", "model-registry"):
            actor_id = None
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT payload FROM public.raw_events
                WHERE environment=%s AND event_id<>%s::uuid
                  AND event_time BETWEEN %s::timestamptz-interval '15 minutes' AND %s::timestamptz+interval '15 minutes'
                  AND (payload #>> '{attributes,tenant_id}') IS NOT DISTINCT FROM %s
                  AND ((%s::text IS NOT NULL AND payload #>> '{subject,model_name}'=%s AND payload #>> '{subject,model_version}'=%s)
                    OR (%s::text IS NULL AND %s::text IS NOT NULL AND payload #>> '{actor,id}'=%s))
                ORDER BY abs(EXTRACT(EPOCH FROM (event_time-%s::timestamptz))), event_id LIMIT 21""",
                (
                    source["environment"],
                    source["event_id"],
                    source["event_time"],
                    source["event_time"],
                    source.get("attributes", {}).get("tenant_id"),
                    model,
                    model,
                    version,
                    model,
                    actor_id,
                    actor_id,
                    source["event_time"],
                ),
            ).fetchall()
        return [row["payload"] for row in rows[:20]], len(rows) > 20

    def reports(self, finding_id=None):
        with self.connection() as conn:
            return serializable(
                conn.execute(
                    """SELECT * FROM investigator.latest_reports
                WHERE (%s::uuid IS NULL OR finding_id=%s::uuid) ORDER BY finished_at DESC LIMIT 20""",
                    (finding_id, finding_id),
                ).fetchall()
            )

    def jobs(self):
        with self.connection() as conn:
            return serializable(
                conn.execute("""SELECT job_id,finding_id,status,attempts,error_code,updated_at
                FROM investigator.jobs ORDER BY updated_at DESC LIMIT 20""").fetchall()
            )

    def failure_code(self, job_id):
        with self.connection() as conn:
            return conn.execute("SELECT error_code FROM investigator.jobs WHERE job_id=%s", (job_id,)).fetchone()[
                "error_code"
            ]


ALLOWED_ATTRIBUTES = frozenset(
    {
        "privileged_role",
        "change_approved",
        "privileged_identity",
        "mfa_used",
        "cluster_role",
        "cross_namespace",
        "privileged",
        "image_approved",
        "dataset_owner",
        "version_bumped",
        "metric_value",
        "threshold",
        "remediation_status",
        "approved_exception",
        "passed",
        "signature_verified",
        "sha256",
        "pii_detected",
        "pii_handling_allowed",
        "model_approved",
        "prompt_injection_score",
        "data_exfiltration_detected",
        "policy_version",
    }
)


def projected_event(raw):
    """Minimize model context: no prompt, excerpt, credentials, IP or arbitrary nested attributes."""

    def scalar(value):
        return (
            value[:300]
            if isinstance(value, str)
            else value
            if isinstance(value, (bool, int, float)) or value is None
            else None
        )

    event = {
        key: scalar(raw.get(key))
        for key in (
            "event_id",
            "event_time",
            "ingested_at",
            "producer",
            "event_type",
            "action",
            "outcome",
            "environment",
            "correlation_id",
        )
    }
    event["actor"] = {"id": scalar(raw.get("actor", {}).get("id"))}
    event["subject"] = {
        key: scalar(raw.get("subject", {}).get(key))
        for key in ("type", "id", "namespace", "model_name", "model_version")
    }
    event["attributes"] = {
        key: scalar(value) for key, value in raw.get("attributes", {}).items() if key in ALLOWED_ATTRIBUTES
    }
    if event["event_type"] == "POD_CREATE":
        # The Kubernetes mapper defaults this RBAC field for every event. It is not
        # a pod approval record or a GOV-006 predicate; retain it on RBAC events only.
        event["attributes"].pop("change_approved", None)
    return serializable(event)


class EvidenceTools:
    def __init__(self, store, finding_id, catalog, policy_hash):
        self.store, self.catalog, self.policy_hash = store, catalog, policy_hash
        self.finding = store.finding(finding_id)
        self.finding["title"] = str(self.finding["title"])[:300]
        self.finding_ref = "finding:" + str(finding_id)
        self.evidence = {self.finding_ref: self.finding}
        self._source = None

    def call(self, name):
        if name == "get_rule":
            ref = f"rule:{self.finding['rule_id']}:{self.policy_hash}"
            result = self.catalog[self.finding["rule_id"]]
            self.evidence[ref] = result
            return {
                "evidence_ref": ref,
                "rule": result,
                "scope": "Configured metadata; not verified compliance or executable predicate text.",
            }
        if self._source is None:
            self._source = self.store.source(self.finding["source_event_id"])
        if name == "get_source_event":
            event = projected_event(self._source)
            ref = "event:" + event["event_id"]
            self.evidence[ref] = event
            return {"evidence_ref": ref, "event": event}
        if name == "get_related_events":
            events, truncated = self.store.related(self._source)
            result = []
            for raw in events:
                event = projected_event(raw)
                ref = "event:" + event["event_id"]
                self.evidence[ref] = event
                result.append({"evidence_ref": ref, "event": event})
            return {
                "events": result,
                "truncated": truncated,
                "scope": "Same environment and tenant marker; matching model or actor; +/-15 minutes; at most 20 nearest events. Not a complete history.",
            }
        raise AgentError("unknown_tool")
