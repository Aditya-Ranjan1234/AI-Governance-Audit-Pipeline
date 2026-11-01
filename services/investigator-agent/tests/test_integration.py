"""Real PostgreSQL, scripted Bedrock responses; never reads production data or calls AWS."""

from dataclasses import replace
import json
import os
import uuid

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb
import pytest

from agent import AgentError
from app import main, process_job
from store import EvidenceTools, LeaseLost, Store, load_catalog
from conftest import EVENT_ID, EVENT_TIME, FINDING_ID, ROOT, ScriptedModel, successful_model

pytestmark = pytest.mark.integration


@pytest.fixture
def database(settings):
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Set TEST_POSTGRES_DSN to an isolated database ending in _test")
    if not conninfo_to_dict(dsn).get("dbname", "").endswith("_test"):
        pytest.fail("Integration tests only allow database names ending in _test")
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute((ROOT / "db/001_schema.sql").read_text())
        c.execute((ROOT / "db/002_investigator.sql").read_text())
        c.execute(
            "TRUNCATE investigator.audit_events, investigator.runs, investigator.jobs, public.compliance_findings, public.raw_events CASCADE"
        )
    config = replace(settings, dsn=dsn)
    catalog, policy_hash = load_catalog(config.rules_path)
    store = Store(config, policy_hash)
    store.check_permissions()
    return store, catalog, config


def seed(store, source=True):
    raw = {
        "schema_version": "1.0",
        "event_id": EVENT_ID,
        "event_time": EVENT_TIME,
        "ingested_at": EVENT_TIME,
        "producer": "MODEL_REGISTRY",
        "event_type": "MODEL_PROMOTED",
        "environment": "prod",
        "correlation_id": "fixture-correlation",
        "actor": {"id": "release-bot"},
        "subject": {"id": "credit:7", "model_name": "credit", "model_version": "7"},
        "attributes": {},
        "outcome": "SUCCESS",
        "action": "promote",
    }
    with psycopg.connect(store.settings.dsn) as c:
        if source:
            c.execute(
                """INSERT INTO raw_events(event_id,schema_version,producer,event_type,event_time,ingested_at,environment,correlation_id,payload)
                VALUES (%s,'1.0','MODEL_REGISTRY','MODEL_PROMOTED',%s,%s,'prod','fixture-correlation',%s)""",
                (EVENT_ID, EVENT_TIME, EVENT_TIME, Jsonb(raw)),
            )
        c.execute(
            """INSERT INTO compliance_findings
            (finding_id,rule_id,title,producer,source_event_id,source_event_time,detected_at,severity,severity_label,frequency,frequency_band,baseline_risk,recommended_risk,frameworks,evidence)
            VALUES (%s,'GOV-011','Unapproved model promotion','MODEL_REGISTRY',%s,%s,%s,5,'critical',1,1,20,20,'{}','{}')""",
            (FINDING_ID, EVENT_ID, EVENT_TIME, EVENT_TIME),
        )
    return raw


def test_complete_investigation_persists_report_evidence_and_audit(database):
    store, catalog, settings = database
    seed(store)
    assert store.enqueue() == 1
    assert store.enqueue() == 0
    job = store.claim()
    assert process_job(store, job, successful_model(), catalog, settings) == "succeeded"
    reports = store.reports()
    assert len(reports) == 1
    assert reports[0]["finding_id"] == FINDING_ID
    assert reports[0]["report"]["assessment"] == "needs_more_evidence"
    assert "event:" + EVENT_ID in reports[0]["evidence"]
    with store.connection() as c:
        kinds = [
            r["kind"] for r in c.execute("SELECT kind FROM investigator.audit_events ORDER BY created_at").fetchall()
        ]
        assert kinds.count("model_response") == 3
        assert kinds[-1] == "report_validated"
        original = c.execute("SELECT recommended_risk,evidence FROM public.compliance_findings").fetchone()
        assert original["recommended_risk"] == 20
        assert original["evidence"] == {}
    assert store.claim() is None


def test_role_cannot_modify_source_or_delete_audit(database):
    store, _, _ = database
    for sql in [
        "DELETE FROM public.raw_events",
        "UPDATE public.compliance_findings SET recommended_risk=0",
        "TRUNCATE public.raw_events",
        "DELETE FROM investigator.audit_events",
    ]:
        with pytest.raises(psycopg.errors.InsufficientPrivilege), store.connection() as c:
            c.execute(sql)


def test_exclusive_claim_and_expired_lease_fencing(database):
    store, _, _ = database
    seed(store)
    store.enqueue()
    first = store.claim()
    assert store.claim() is None
    with psycopg.connect(store.settings.dsn) as c:
        c.execute("UPDATE investigator.jobs SET lease_until=now()-interval '1 second'")
    second = store.claim()
    assert second["attempts"] == 2
    assert second["lease_token"] != first["lease_token"]
    with pytest.raises(LeaseLost):
        store.finish(first, report={"stale": True})
    with pytest.raises(LeaseLost):
        store.audit(first, {"kind": "stale"})
    store.finish(second, error=AgentError("test_failed"))
    assert store.jobs()[0]["status"] == "failed"


def test_missing_source_retries_without_producing_a_report(database):
    store, catalog, settings = database
    seed(store, source=False)
    store.enqueue()
    model = successful_model()
    state = process_job(store, store.claim(), model, catalog, settings)
    assert state == "retry"
    assert store.jobs()[0]["error_code"] == "source_event_unavailable"
    assert store.reports() == []
    # Finding is provided to the model, which requests the source; no false report is produced.
    assert len(model.requests) == 1


def test_transient_provider_error_retries_and_permanent_error_terminates(database):
    store, catalog, settings = database
    seed(store)
    store.enqueue()
    assert (
        process_job(store, store.claim(), ScriptedModel([AgentError("temporary", True)]), catalog, settings) == "retry"
    )
    assert store.claim() is None  # backoff prevents hot retry loops
    with psycopg.connect(settings.dsn) as c:
        c.execute("UPDATE investigator.jobs SET available_at=now()-interval '1 second'")
    assert (
        process_job(store, store.claim(), ScriptedModel([AgentError("denied", False)]), catalog, settings) == "failed"
    )
    assert store.claim() is None


def test_abandoned_last_attempt_becomes_terminal(database):
    store, _, settings = database
    seed(store)
    store.enqueue()
    store.claim()
    with psycopg.connect(settings.dsn) as c:
        c.execute(
            "UPDATE investigator.jobs SET attempts=%s, lease_until=now()-interval '1 second'", (settings.max_attempts,)
        )
    assert store.claim() is None
    assert store.jobs()[0]["error_code"] == "lease_expired"


def test_related_events_respect_model_environment_tenant_and_limit(database):
    store, catalog, settings = database
    raw = seed(store)
    with psycopg.connect(settings.dsn) as c:
        for n in range(25):
            value = dict(raw, event_id=str(uuid.uuid4()), event_type="APPROVAL_GRANTED")
            environment = "stage" if n == 0 else "prod"
            if n == 1:
                value["attributes"] = {"tenant_id": "different-tenant"}
            if n == 2:
                value["subject"] = {"model_name": "different-model", "model_version": "7"}
            c.execute(
                """INSERT INTO raw_events(event_id,schema_version,producer,event_type,event_time,ingested_at,environment,correlation_id,payload)
                VALUES (%s,'1.0','MODEL_REGISTRY','APPROVAL_GRANTED',%s,%s,%s,'fixture',%s)""",
                (value["event_id"], EVENT_TIME, EVENT_TIME, environment, Jsonb(value)),
            )
    tools = EvidenceTools(store, FINDING_ID, catalog, store.policy_hash)
    result = tools.call("get_related_events")
    assert len(result["events"]) == 20
    assert result["truncated"] is True
    assert all(item["event"]["subject"]["model_name"] == "credit" for item in result["events"])


def test_policy_or_model_version_gets_a_separate_job(database):
    store, _, settings = database
    seed(store)
    assert store.enqueue() == 1
    assert Store(replace(settings, model_id="another-model"), store.policy_hash).enqueue() == 1
    assert Store(settings, "different-policy-hash").enqueue() == 1
    assert len(store.jobs()) == 3


def test_narrowed_rule_scope_does_not_claim_older_queued_findings(database):
    store, _, settings = database
    seed(store)
    store.enqueue()
    narrowed = Store(replace(settings, rules=("GOV-002",)), store.policy_hash)
    assert narrowed.claim() is None
    assert store.claim() is not None


def test_migration_is_repeatable_without_losing_results(database):
    store, catalog, settings = database
    seed(store)
    store.enqueue()
    process_job(store, store.claim(), successful_model(), catalog, settings)
    with psycopg.connect(settings.dsn, autocommit=True) as c:
        c.execute((ROOT / "db/002_investigator.sql").read_text())
    assert len(store.reports()) == 1


def test_cli_shows_and_reuses_saved_report_without_model_call(database, monkeypatch, capsys):
    store, catalog, settings = database
    seed(store)
    store.enqueue()
    process_job(store, store.claim(), successful_model(), catalog, settings)
    capsys.readouterr()
    monkeypatch.setenv("INVESTIGATOR_POSTGRES_DSN", settings.dsn)
    monkeypatch.delenv("BEDROCK_MODEL_ID", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)

    def no_model(*args, **kwargs):
        pytest.fail("Saved results must not create a Bedrock client")

    monkeypatch.setattr("app.BedrockModel", no_model)
    assert main(["show", FINDING_ID]) == 0
    assert json.loads(capsys.readouterr().out)[0]["report"] == store.reports()[0]["report"]
    monkeypatch.setenv("BEDROCK_MODEL_ID", settings.model_id)
    monkeypatch.setenv("AWS_REGION", settings.region)
    assert main(["investigate", FINDING_ID]) == 0
    assert json.loads(capsys.readouterr().out)[0]["finding_id"] == FINDING_ID
    assert len(store.jobs()) == 1


def test_worker_stops_after_a_systemic_model_configuration_failure(database, monkeypatch, capsys):
    store, _, settings = database
    seed(store)
    monkeypatch.setenv("INVESTIGATOR_POSTGRES_DSN", settings.dsn)
    monkeypatch.setenv("BEDROCK_MODEL_ID", settings.model_id)
    monkeypatch.setenv("AWS_REGION", settings.region)
    monkeypatch.setattr("app.signal.signal", lambda *_: None)
    monkeypatch.setattr("app.BedrockModel", lambda _: ScriptedModel([AgentError("bedrock_AccessDeniedException")]))
    assert main(["worker"]) == 2
    assert store.jobs()[0]["error_code"] == "bedrock_AccessDeniedException"
    assert "bedrock_configuration_error" in capsys.readouterr().err
