"""Real database tests for the complete two-agent message exchange; no AWS calls."""

from dataclasses import replace
import uuid

import psycopg
from psycopg.types.json import Jsonb
import pytest

from agent import AgentError
from app import main as investigator_main, process_job
from conftest import ROOT, ScriptedModel, report, successful_model
from review_store import REVIEWER_VERSION, REVISION_VERSION, TaskStore
from review_worker import process_task
from reviewer_app import main as reviewer_main
from store import LeaseLost
from test_integration import database, seed  # noqa: F401 -- imported pytest fixture
from test_reviewer import decision, review_model

pytestmark = pytest.mark.integration


@pytest.fixture
def workflow(database):  # noqa: F811 -- pytest injects the imported fixture
    original, catalog, settings = database
    with psycopg.connect(settings.dsn, autocommit=True) as c:
        c.execute((ROOT / "db/003_reviewer.sql").read_text())
    seed(original)
    original.enqueue()
    assert process_job(original, original.claim(), successful_model(), catalog, settings) == "succeeded"
    root = original.reports()[0]["run_id"]
    reviewer = TaskStore(settings, reviewing=True)
    investigator = TaskStore(settings, reviewing=False)
    reviewer.check_permissions()
    investigator.check_permissions()
    return original, reviewer, investigator, root, settings


def request_revision(reviewer, root):
    assert reviewer.enqueue(root) == 1
    job = reviewer.claim(root)
    assert process_task(reviewer, job, review_model(decision("revision_required"))) == "succeeded"
    return job


def test_both_agents_exchange_cited_feedback_and_one_revision(workflow):
    original, reviewer, investigator, root, settings = workflow
    before = original.reports()[0]
    request_revision(reviewer, root)
    assert reviewer.results(root)[0]["verdict"] == "revision_required"
    revision = investigator.claim(root)
    assert revision["source_run_id"] == uuid.UUID(root)
    updated = report()
    updated["summary"]["text"] = "No approval was present in the bounded snapshot; approval elsewhere is unknown."
    assert process_task(investigator, revision, successful_model(updated)) == "succeeded"
    assert reviewer.enqueue(root) == 1
    final_job = reviewer.claim(root)
    assert final_job["revision_run_id"] is not None
    assert process_task(reviewer, final_job, review_model()) == "succeeded"
    final = reviewer.results(root)[0]
    assert final["verdict"] == "review_passed"
    assert final["report"] == updated
    assert original.reports()[0] == before
    assert reviewer.enqueue(root) == 0
    assert reviewer.claim() is None and investigator.claim() is None
    with psycopg.connect(settings.dsn) as c:
        assert c.execute("SELECT count(*) FROM investigator.revision_jobs").fetchone()[0] == 1
        assert c.execute("SELECT count(*) FROM reviewer.jobs").fetchone()[0] == 2
        assert (
            c.execute("SELECT count(*) FROM reviewer.audit_events WHERE details ? 'revision_request_id'").fetchone()[0]
            == 1
        )


def test_second_revision_is_escalated_and_database_rejects_another_request(workflow):
    _, reviewer, investigator, root, settings = workflow
    request_revision(reviewer, root)
    process_task(investigator, investigator.claim(), successful_model())
    reviewer.enqueue(root)
    process_task(reviewer, reviewer.claim(), review_model(decision("revision_required")))
    final = reviewer.results(root)[0]
    assert final["verdict"] == "needs_human_review"
    assert final["review"]["revision_limit_reached"] is True
    assert investigator.claim() is None
    with pytest.raises(psycopg.errors.CheckViolation), reviewer.connection() as c:
        c.execute(
            """INSERT INTO investigator.revision_jobs(job_id,source_run_id,review_run_id,worker_version,model_id)
            VALUES(%s,%s,%s,%s,%s)""",
            (uuid.uuid4(), root, final["review_run_id"], REVISION_VERSION, settings.model_id),
        )


def test_duplicate_finish_cannot_duplicate_revision_message(workflow):
    _, reviewer, investigator, root, _ = workflow
    job = request_revision(reviewer, root)
    with pytest.raises(LeaseLost):
        reviewer.finish(job, payload={"effective_verdict": "revision_required"})
    assert len(investigator.jobs()) == 1


def test_reviewer_and_investigator_permissions_keep_results_immutable(workflow):
    _, reviewer, investigator, root, _ = workflow
    request_revision(reviewer, root)
    denied = [
        (reviewer, "SELECT * FROM public.raw_events"),
        (reviewer, "UPDATE investigator.runs SET report='{}'"),
        (reviewer, "DELETE FROM reviewer.audit_events"),
        (reviewer, "UPDATE reviewer.jobs SET source_run_id=source_run_id"),
        (investigator, "UPDATE reviewer.runs SET payload='{}'"),
        (investigator, "DELETE FROM investigator.revision_runs"),
        (investigator, "UPDATE investigator.revision_jobs SET source_run_id=source_run_id"),
    ]
    for store, sql in denied:
        with pytest.raises(psycopg.errors.InsufficientPrivilege), store.connection() as c:
            c.execute(sql)


@pytest.mark.parametrize("reviewing", [True, False])
def test_exclusive_claim_and_expired_worker_cannot_write(workflow, reviewing):
    _, reviewer, investigator, root, settings = workflow
    if reviewing:
        reviewer.enqueue(root)
    else:
        request_revision(reviewer, root)
    store = reviewer if reviewing else investigator
    first = store.claim()
    assert store.claim() is None
    with psycopg.connect(settings.dsn) as c:
        c.execute(
            f"UPDATE {store.prefix}jobs SET lease_until=now()-interval '1 second' WHERE job_id=%s", (first["job_id"],)
        )
    second = store.claim()
    assert second["attempts"] == 2
    assert second["lease_token"] != first["lease_token"]
    with pytest.raises(LeaseLost):
        store.audit(first, {"kind": "stale"})
    with pytest.raises(LeaseLost):
        store.finish(first, payload={})


def test_transient_review_failure_retries_without_issuing_feedback(workflow):
    _, reviewer, investigator, root, settings = workflow
    reviewer.enqueue(root)
    assert process_task(reviewer, reviewer.claim(), ScriptedModel([AgentError("temporary", True)])) == "retry"
    assert investigator.jobs() == []
    assert reviewer.claim() is None
    with psycopg.connect(settings.dsn) as c:
        c.execute("UPDATE reviewer.jobs SET available_at=now()-interval '1 second'")
    assert process_task(reviewer, reviewer.claim(), review_model()) == "succeeded"
    assert reviewer.results(root)[0]["verdict"] == "review_passed"


def test_final_expired_attempt_becomes_failed_without_model_calls(workflow):
    _, reviewer, _, root, settings = workflow
    reviewer.enqueue(root)
    reviewer.claim()
    with psycopg.connect(settings.dsn) as c:
        c.execute(
            "UPDATE reviewer.jobs SET attempts=%s,lease_until=now()-interval '1 second'", (settings.max_attempts,)
        )
    assert reviewer.claim() is None
    assert reviewer.jobs()[0]["error_code"] == "lease_expired"


def test_cross_chain_revision_review_is_rejected(workflow):
    _, reviewer, investigator, root, settings = workflow
    request_revision(reviewer, root)
    process_task(investigator, investigator.claim(), successful_model())
    with psycopg.connect(settings.dsn) as c:
        run_id = c.execute("SELECT run_id FROM investigator.revision_runs").fetchone()[0]
    with pytest.raises(psycopg.errors.CheckViolation), reviewer.connection() as c:
        c.execute(
            """INSERT INTO reviewer.jobs(job_id,source_run_id,revision_run_id,worker_version,model_id,revision_model_id)
            VALUES(%s,%s,%s,%s,'different-reviewer',%s)""",
            (uuid.uuid4(), root, run_id, REVIEWER_VERSION, settings.model_id),
        )


def test_narrowed_scope_does_not_claim_existing_out_of_scope_work(workflow):
    _, reviewer, _, root, settings = workflow
    reviewer.enqueue(root)
    narrow = TaskStore(replace(settings, rules=("GOV-003",)), reviewing=True)
    assert narrow.claim() is None
    assert reviewer.claim() is not None


def test_repeatable_migration_preserves_original_and_review(workflow):
    original, reviewer, _, root, settings = workflow
    reviewer.enqueue(root)
    process_task(reviewer, reviewer.claim(), review_model())
    with psycopg.connect(settings.dsn, autocommit=True) as c:
        c.execute((ROOT / "db/003_reviewer.sql").read_text())
    assert len(reviewer.results(root)) == len(original.reports()) == 1


def test_cli_review_and_revision_exchange_without_starting_business_pipeline(workflow, monkeypatch, capsys):
    _, reviewer, _, root, settings = workflow
    monkeypatch.setenv("REVIEWER_POSTGRES_DSN", settings.dsn)
    monkeypatch.setenv("INVESTIGATOR_POSTGRES_DSN", settings.dsn)
    monkeypatch.setenv("REVIEWER_MODEL_ID", settings.model_id)
    monkeypatch.setenv("BEDROCK_MODEL_ID", settings.model_id)
    monkeypatch.setenv("AWS_REGION", settings.region)
    monkeypatch.setattr("reviewer_app.workflow_model", lambda *a, **kw: review_model(decision("revision_required")))
    assert reviewer_main(["review", root]) == 0
    monkeypatch.setattr("app.workflow_model", lambda *a, **kw: successful_model())
    assert investigator_main(["revise-once"]) == 0
    monkeypatch.setattr("reviewer_app.workflow_model", lambda *a, **kw: review_model())
    assert reviewer_main(["review", root]) == 0
    assert reviewer.results(root)[0]["revision_run_id"] is not None
    assert reviewer.results(root)[0]["verdict"] == "review_passed"


def test_reviewer_stops_on_provider_configuration_error(workflow, monkeypatch, capsys):
    _, reviewer, _, _, settings = workflow
    monkeypatch.setenv("REVIEWER_POSTGRES_DSN", settings.dsn)
    monkeypatch.setenv("REVIEWER_MODEL_ID", settings.model_id)
    monkeypatch.setenv("AWS_REGION", settings.region)
    monkeypatch.setattr(
        "reviewer_app.workflow_model", lambda *a, **kw: ScriptedModel([AgentError("bedrock_AccessDeniedException")])
    )
    assert reviewer_main(["worker"]) == 2
    assert reviewer.jobs()[0]["status"] == "failed"
    assert "bedrock_configuration_error" in capsys.readouterr().err


def test_discovery_reviews_latest_original_only_and_does_not_backfill_history(workflow):
    original, reviewer, _, root, settings = workflow
    with psycopg.connect(settings.dsn) as c:
        old_job = c.execute("SELECT job_id FROM investigator.runs WHERE run_id=%s", (root,)).fetchone()[0]
        newer = uuid.uuid4()
        c.execute(
            """INSERT INTO investigator.runs(run_id,job_id,attempt,status,report,evidence,finished_at)
            SELECT %s,%s,2,'succeeded',report,evidence,now()+interval '1 second' FROM investigator.runs WHERE run_id=%s""",
            (newer, old_job, root),
        )
    assert reviewer.enqueue() == 1
    job = reviewer.claim()
    assert job["source_run_id"] == newer
    process_task(reviewer, job, review_model())
    assert reviewer.enqueue() == 0
    assert len(reviewer.jobs()) == 1
    assert len(original.reports()) == 1


def test_failed_selected_configuration_is_not_hidden_by_another_models_review(workflow):
    _, reviewer, _, root, settings = workflow
    reviewer.enqueue(root)
    process_task(reviewer, reviewer.claim(), review_model())
    alternate = TaskStore(replace(settings, model_id="different-review-model"), reviewing=True)
    alternate.enqueue(root)
    process_task(alternate, alternate.claim(), ScriptedModel([AgentError("permanent_failure")]))
    assert reviewer.selected_complete(root)
    assert not alternate.selected_complete(root)


def test_deterministic_contradiction_requests_revision_even_when_model_passes(workflow):
    _, reviewer, investigator, root, settings = workflow
    with psycopg.connect(settings.dsn) as c:
        saved = c.execute("SELECT report,evidence FROM investigator.runs WHERE run_id=%s", (root,)).fetchone()
        value, evidence = saved
        ref = next(ref for ref in evidence if ref.startswith("event:"))
        evidence[ref].update(event_type="LOGIN_SUCCESS", attributes={"mfa_used": False})
        value["observations"][0]["text"] = "An unauthenticated privileged login occurred."
        # Test owner changes a synthetic fixture before the reviewer reads it; agent roles cannot do this.
        c.execute(
            "UPDATE investigator.runs SET report=%s,evidence=%s WHERE run_id=%s", (Jsonb(value), Jsonb(evidence), root)
        )
    reviewer.enqueue(root)
    process_task(reviewer, reviewer.claim(), review_model())
    result = reviewer.results(root)[0]
    assert result["review"]["review"]["verdict"] == "review_passed"
    assert result["verdict"] == "revision_required"
    assert len(investigator.jobs()) == 1
