# Investigator and Evidence Reviewer

The project now has two product agents. The investigator explains a persisted finding. The reviewer checks that explanation against the exact evidence used to write it. The reviewer can request one correction, which the investigator saves as a new report revision. Both use Amazon Bedrock Converse; they run as separate processes with different prompts, tool contracts and database roles. They can use different configured models.

The existing adapters, Kafka Streams processor, scorer and sink remain ordinary services. Neither agent performs remediation or changes source events, findings or risk scores. A review verdict is advisory; it is not compliance certification.

```mermaid
sequenceDiagram
    participant I as Investigator
    participant P as PostgreSQL
    participant R as Evidence Reviewer
    I->>P: Save original report and evidence snapshot
    R->>P: Discover and lease review job
    P-->>R: Report and immutable evidence, keyed by source_run_id
    R->>P: Save cited review and verdict
    alt Correction required on initial review
        R->>P: Atomically enqueue revision request
        I->>P: Poll and lease revision request
        P-->>I: Original report, evidence and review feedback
        I->>P: Save new revision, preserving original
        R->>P: Discover and lease revision review
        R->>P: Save review_passed or needs_human_review
    end
```

## Exact communication contract

Communication is asynchronous database polling. There is no direct agent HTTP endpoint and no additional Kafka topic. Bedrock returns tool requests to the relevant worker; application code executes only its fixed tools.

| Message | Durable location | Identity and contents | Consumer |
|---|---|---|---|
| Original investigation | `investigator.runs` | Original `run_id`, report and immutable evidence snapshot | Reviewer |
| Review task | `reviewer.jobs` | `source_run_id`, optional `revision_run_id`, reviewer version/model and investigator model | Reviewer |
| Review decision | `reviewer.runs` | Model review, deterministic issues, effective verdict and revision-limit flag | Investigator / user |
| Correction request | `investigator.revision_jobs` | `source_run_id`, `review_run_id`, revision worker version and target model | Investigator |
| Corrected report | `investigator.revision_runs` | New `run_id`, complete revised report and original evidence snapshot | Reviewer |
| Current reviewed result | `reviewer.latest_results` | Original or revised report, review result, IDs and provenance | CLI / reporting |

Initial review jobs are unique by original run, reviewer version, reviewer model and revision model. Follow-up review jobs use the revision run plus the same configuration. A correction request is unique by its initial review result. Polling discovers only the latest successful original report per finding; explicit `review RUN_UUID` can select an older successful run. Discovery batches contain at most 100 new originals and 100 new revisions.

Every worker claims one job using `FOR UPDATE SKIP LOCKED` and a lease token. A stale or expired worker cannot append a result. Saving the first review and its correction request happens in one transaction, so a crash cannot leave a saved correction decision without its outgoing request. A saved revision is discovered idempotently by the reviewer after either process restarts.

Review feedback has this structure:

```json
{
  "review": {
    "verdict": "revision_required",
    "summary": "The report reverses the order of two events.",
    "issues": [{
      "claim_path": "/observations/1/text",
      "code": "chronology",
      "explanation": "The cited login occurred after the source role grant.",
      "evidence_refs": ["event:SOURCE_UUID", "event:LOGIN_UUID"],
      "requested_correction": "Describe the recorded event order without inferring a shared session."
    }]
  },
  "deterministic_issues": [],
  "effective_verdict": "revision_required",
  "revision_limit_reached": false
}
```

The UUID strings above are placeholders. Runtime validation requires real references from the snapshot and a claim path that points to existing report prose. Feedback is data, not an instruction that can grant additional tools or access.

## Review and revision behavior

The reviewer has three tools: `get_report`, `get_evidence`, and `submit_review`. It must read both inputs before submitting. Its checks cover factual support, chronology, authentication/MFA, approval semantics, bounded-query absence and unsupported session/causal claims. Proposed actions and explicitly labelled hypotheses are not treated as executed actions or established facts.

The reviewer receives the target finding and an explicit map of editable claim paths. It must judge the report's actual claims against that finding's pinned rule; omitting unrelated events is not itself an error. These prompt constraints reduce scope drift but do not guarantee that the model follows them.

Conservative deterministic checks independently catch two observed classes: calling a cited successful non-MFA login unauthenticated, and explicitly claiming a cited login preceded a source role grant when its timestamp is later. These checks do not solve general natural-language verification. Their findings remain in the result and can force `revision_required` even if the model returns `review_passed`.

The investigator uses its existing four-tool interface in revision mode, but reads exclusively from the original snapshot. It receives the saved report and cited feedback, and must produce a complete, validated replacement. It cannot retrieve newer live events during this correction cycle. Reports needing new evidence can remain for human review or a separately initiated investigation.

Effective verdicts:

- `review_passed`: no issues identified by the model or deterministic checks; still advisory.
- `revision_required`: the initial review requests a single correction.
- `needs_human_review`: evidence cannot settle a material issue, or the reviewer still wants corrections after the one allowed revision.

The application converts a second correction request to `needs_human_review`. Database triggers also reject a revision request tied to a follow-up review or the wrong source chain. Model calls have bounded turns, token limits, deadlines and retries. Provider/configuration failures remain visible failed jobs and never become passing reviews. Permanent model configuration errors stop the affected worker. Audit entries retain job attempts, tool activity, usage and outgoing revision-request IDs, without logging rejected model content or provider error messages.

## Run locally

The business pipeline must already have its schema and persisted investigations. Migration 003 is additive; do not delete the PostgreSQL volume.

```bash
# As database owner; app.py migrate applies both 002 and 003.
export INVESTIGATOR_POSTGRES_DSN='OWNER_DSN'
.venv/bin/python services/investigator-agent/app.py migrate
```

Provision dedicated logins through your database/secret-management process:

```sql
CREATE ROLE investigator_login LOGIN;
CREATE ROLE reviewer_login LOGIN;
GRANT governance_investigator TO investigator_login;
GRANT governance_reviewer TO reviewer_login;
```

Set their passwords outside committed configuration. The investigator can read source tables and append its reports/revisions. The reviewer reads saved evidence snapshots; it has no direct `raw_events` access and cannot modify original investigations. Each role owns only its workflow writes. Queue routing fields and saved results are immutable to the runtime roles.

For two host terminals:

```bash
# Shared AWS configuration; host mode supports AWS_PROFILE.
export AWS_PROFILE='YOUR_PROFILE'
export AWS_REGION='us-west-2'
export BEDROCK_MODEL_ID='YOUR_INVESTIGATOR_MODEL_OR_PROFILE'
export REVIEWER_MODEL_ID='YOUR_REVIEWER_MODEL_OR_PROFILE'
export INVESTIGATOR_POSTGRES_DSN='RESTRICTED_INVESTIGATOR_DSN'
export REVIEWER_POSTGRES_DSN='RESTRICTED_REVIEWER_DSN'
export INVESTIGATOR_REVISIONS_ENABLED=true

# Terminal 1: original investigations plus incoming revision requests.
.venv/bin/python services/investigator-agent/app.py worker

# Terminal 2: review original reports and returned revisions.
.venv/bin/python services/investigator-agent/reviewer_app.py worker
```

To exercise one exchange explicitly:

```bash
.venv/bin/python services/investigator-agent/reviewer_app.py review ORIGINAL_RUN_UUID
.venv/bin/python services/investigator-agent/app.py revise-once
.venv/bin/python services/investigator-agent/reviewer_app.py review ORIGINAL_RUN_UUID
.venv/bin/python services/investigator-agent/reviewer_app.py show ORIGINAL_RUN_UUID
```

`review` processes at most one available review for the selected root. `revise-once` processes at most one queued revision across the configured scope. Use the two workers to drain an entire queue. The original `app.py show` still displays original investigations; use `reviewer_app.py show` for reviewed reports and revisions. `status` on the reviewer CLI shows its jobs; revision tasks are in `investigator.revision_jobs`.

For Compose, supply temporary AWS credentials through the environment and the two database DSNs; then:

```bash
docker compose --profile agents up --build -d investigator-agent reviewer-agent
docker compose --profile agents run --rm --no-deps reviewer-agent show
```

The profile enables revisions on the investigator and starts the reviewer as a separate container using the same built image. Compose's development default uses the database owner login and switches to the restricted runtime role; use distinct logins outside local development. Host AWS profile files are not mounted into containers.

## Configuration

| Setting | Purpose / default |
|---|---|
| `INVESTIGATOR_REVISIONS_ENABLED` | `true` enables correction requests in the normal investigator worker; host default `false`, Compose profile enables it |
| `REVIEWER_POSTGRES_DSN` | Reviewer database login; required in host mode |
| `REVIEWER_MODEL_ID` | Reviewer Converse model/profile; required for host model calls; Compose falls back to `BEDROCK_MODEL_ID` |
| `BEDROCK_MODEL_ID` | Investigator model and target model written on revision requests; reviewer host mode falls back to its own model if omitted |
| `REVIEWER_RULE_IDS` | Default GOV-002,003,004,006,011,012 |
| `REVIEWER_POLL_SECONDS` | 5; allowed 1–300 |
| `REVIEWER_MAX_STEPS` | 6; allowed 2–12 |
| `REVIEWER_MAX_ATTEMPTS` | 3; allowed 1–5 |
| `REVIEWER_TIMEOUT_SECONDS` | 120; allowed 10–300 |
| `REVIEWER_MAX_TOKENS` | 3,000; allowed 256–4,096 |

Investigator revision calls use `INVESTIGATOR_*` limits. A correction request's model must match the running investigator's `BEDROCK_MODEL_ID` to be claimed. Review configuration identity includes both model IDs. Each agent retains an independent conversation; they exchange saved outputs and cited feedback, not model reasoning traces.

The reviewer and investigator revision loop wait their configured polling interval after each task. Bedrock throttling schedules a retry after 60 seconds, doubling up to 300 seconds, and pauses the affected worker for at least 60 seconds. This is backoff, not a shared request-rate limiter: each task can make multiple Converse calls, and both agents consume the account/model quota. Set both polling intervals to suit that quota and the number of replicas.

Optional EKS manifests are in `deploy/k8s/investigator.yaml` and `deploy/k8s/reviewer.yaml`. They use separate service accounts, IAM-role placeholders and database secrets. Building this feature does not deploy it to AWS or change IAM permissions.

## Tests and evaluation

The automated suite uses scripted models, botocore Stubber and a real isolated PostgreSQL database. It tests the complete review/correction/recheck exchange, source/result permissions, duplicate discovery, replay, stale leases, retries, cross-chain references, escalation and known regression cases.

The November 2 validation passed **83 automated tests**, lint/format checks, Compose validation and the packaged container build. The live test used Amazon Nova 2 Lite (`us.amazon.nova-2-lite-v1:0`) in `us-west-2`, separate restricted database logins, and two containers running as UID 10001 with read-only filesystems and all Linux capabilities dropped. The account's observed cross-region model quota was 20 requests/minute; both worker polling intervals were set to 20 seconds and both output limits to 3,000 tokens. These are measured test settings, not claims about every AWS account.

The [standalone results page](two-agent-results-2026-11-02.html) and its [JSON evidence](two-agent-results-2026-11-02.json) contain all 12 test cases, feedback, revisions, audit history and earlier trials. The reviewer challenged six of eight known-problem reports, but also three of four clean controls. Some challenges gave an incorrect diagnosis, and one incorrect reviewer suggestion introduced a chronology error into a previously clean report. Workflow completion is therefore **not a quality pass**. Keep this model configuration optional and advisory, with human verification of both feedback and revised reports. Additional evidence inspection is recorded in `evaluation/reviewer-inspection-2026-11-02.json`.

All **30 workflow jobs** completed: 12 initial reviews, nine investigator revisions and nine rechecks. Latest outcomes were four advisory passes and eight `needs_human_review` results. Originals and evidence snapshots remained unchanged, and every review chain stayed within one revision. The final recheck caught the chronology error introduced by incorrect feedback and escalated it. Two original known-problem reports still received passing reviews, so the four passes must not be interpreted as four independently verified reports.

The live run also exercised recovery: its final recheck exhausted three attempts after transport connection failures. An operator audit event records refreshing temporary credentials and requeuing only that job with a maximum of five total attempts, without deleting its failed run records. Failed jobs are otherwise terminal; workers do not silently reset attempt limits.

```bash
TEST_POSTGRES_DSN='postgresql://USER:PASSWORD@127.0.0.1:5432/governance_review_test' \
  .venv/bin/pytest -q services/investigator-agent/tests
.venv/bin/ruff check services/investigator-agent
.venv/bin/ruff format --check services/investigator-agent
docker compose --profile agents config --quiet
```

Database test names must end in `_test`; those fixtures clear only that explicitly selected test database. The live evaluation helper `evaluation/reviewer_evaluation.py` requires a localhost database ending in `_review_e2e`. It imports eight saved problem reports and constructs four clean controls without sending ground-truth labels or prior review notes to either agent. The seed and report commands themselves never call Bedrock; run the two packaged workers against that isolated database to produce live results.

The new agent's source is beside the investigator so the shared bounded tool runner, Bedrock client and dependency image stay consistent: `reviewer.py` defines both roles' prompts and snapshot tools; `review_contracts.py` validates cited decisions; `review_store.py` implements durable communication; `review_worker.py` dispatches tasks; `reviewer_app.py` is the second process's CLI. The database contract is enforced in `db/003_reviewer.sql`.
