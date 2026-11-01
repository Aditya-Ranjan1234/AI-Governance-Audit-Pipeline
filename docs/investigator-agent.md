# One read-only investigator

The optional investigator uses Amazon Bedrock Converse to investigate **persisted findings**. It has four tools: read the source event, read related events, read configured rule metadata, and submit a structured advisory report. It cannot execute SQL supplied by a model, modify a source system, change a finding's risk score, send a message, or perform remediation.

```text
Existing Kafka pipeline → PostgreSQL findings
                                ↓
                   discover / deduplicate jobs
                                ↓
                  one Bedrock investigator
                 source event · related events · rule
                                ↓
                validated, cited advisory report
                                ↓
          investigator.latest_reports + audit_events
```

Database polling deliberately starts after finding persistence. This first version adds no Kafka topic/consumer, agent framework, web interface, or automated action executor. Existing services remain independent. The `agents` Compose profile is opt-in.

## Start locally

Prerequisites: the existing pipeline's PostgreSQL database/schema, AWS credentials, and a Bedrock model or inference profile that supports **Converse tool use** in your chosen region. There is no hardcoded model choice and no fake production fallback.

For the host CLI (convenient with an AWS CLI named profile):

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r services/investigator-agent/requirements.txt
export INVESTIGATOR_POSTGRES_DSN='postgresql://governance:governance@127.0.0.1:5432/governance'
export AWS_REGION='us-west-2'
export AWS_PROFILE='YOUR_AWS_PROFILE'
export BEDROCK_MODEL_ID='YOUR_ENABLED_MODEL_OR_INFERENCE_PROFILE_ID'

# One-time additive migration, as database owner with CREATEROLE:
.venv/bin/python services/investigator-agent/app.py migrate

# Investigate one existing eligible finding (replace the UUID):
.venv/bin/python services/investigator-agent/app.py investigate FINDING_UUID

# Or discover queued findings and process at most one:
.venv/bin/python services/investigator-agent/app.py once

# Or keep the single investigator running:
.venv/bin/python services/investigator-agent/app.py worker

# Inspect results; these commands make no model calls:
.venv/bin/python services/investigator-agent/app.py show
.venv/bin/python services/investigator-agent/app.py status
```

For Compose, set `AWS_REGION` and `BEDROCK_MODEL_ID` in the project `.env` or shell. Supply temporary AWS credentials through exported `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`; Compose forwards these values to the agent. The host's AWS_PROFILE/config files are **not** mounted automatically.

```bash
docker compose --profile agents up --build -d investigator-agent
docker compose --profile agents run --rm --no-deps investigator-agent show
docker compose --profile agents run --rm --no-deps investigator-agent status
```

The init service applies migration 002 to existing databases, so existing PostgreSQL volumes work without deletion. Compose's local default uses the development database login, then always switches the runtime connection to the restricted `governance_investigator` role. Use a dedicated login in production as below. `INVESTIGATOR_ADMIN_DSN` overrides the Compose migration connection; `INVESTIGATOR_POSTGRES_DSN` overrides the runtime connection.

Default scope: **GOV-002, GOV-003, GOV-004, GOV-006, GOV-011, GOV-012** (privileged access/workloads and model promotion). Only findings already stored in `compliance_findings` are candidates. To generate test findings through the existing pipeline, use its evaluation command; the investigator never silently generates business data.

## Output and verification

A saved report contains:

- `summary` and `observations`, each with `evidence_refs`.
- A `timeline` tied to retrieved event IDs and exact recorded timestamps.
- `missing_evidence` and `recommended_next_steps` for a reviewer.
- `assessment`: `evidence_review_complete` or `needs_more_evidence`; neither resolves a finding or certifies compliance.

`investigator.latest_reports` exposes the most recent successful report per finding, joined to its current title/risk fields. `investigator.runs` retains versioned report/evidence snapshots; `investigator.jobs` tracks lifecycle; `investigator.audit_events` records attempts, model token usage, tool reads/rejections and validated submissions. Model chain-of-thought and raw prompts are not persisted. Reports remain model-generated advisory text: citation validation proves reference membership, not that every statement is entailed by its reference.

## Engineering boundaries

- **Source access:** parameterized queries run under `SET LOCAL ROLE governance_investigator`. Startup refuses source-table INSERT/UPDATE/DELETE/TRUNCATE privileges. The role can only read domain tables and write the dedicated investigator tables; it cannot delete audit rows.
- **Durability:** job uniqueness covers finding ID, agent version, rules-file SHA-256 and model ID. Repeated discovery/CLI requests reuse successful work. Worker claims use `FOR UPDATE SKIP LOCKED` and lease tokens. Stale workers cannot finalize a reclaimed job.
- **Recovery:** transient Bedrock errors and unavailable source rows receive bounded retries with backoff. Database failures leave the lease for later recovery. Permanent/model-protocol failures become visible failed jobs. An abandoned final attempt is marked failed when its lease expires. Failed jobs do not loop indefinitely; inspect `status` and correct the cause before an operator-directed requeue or new version/model run.
- **Bounds:** six model turns, four tool calls per turn, 120-second investigation deadline, 1,500 output tokens per call, 100,000 serialized context characters, and at most 20 related events. Network calls have 5-second connect/30-second read timeouts and two total SDK attempts. Model responses arriving after the deadline are rejected; an in-flight network call may finish after that logical deadline. Leases include an additional 150 seconds.
- **Context:** related events are within ±15 minutes, the same environment and tenant marker, and the same model/version (or actor if no model is available). Missing tenant tags do not establish tenant isolation. This MVP is for one trusted deployment scope; expose no public or cross-tenant agent API.
- **Data minimization:** source projections exclude IPs, raw prompts, excerpts, credentials and arbitrary attributes. IDs and allowlisted governance fields are still data sent to Bedrock; this is not a general anonymizer. All retrieved values are treated as untrusted data. No model-controlled tool can broaden SQL scope.
- **Evidence gaps:** source rows can arrive after findings. The agent reports/retries unavailable evidence rather than inventing it. A bounded history is never proof that an approval did not exist; event timestamps do not prove Kafka processing order.
- **Versioning:** reports use a snapshot of retrieved evidence and catalog metadata. Score upserts alone do not enqueue another analysis. Executable rule predicates remain in Java; `get_rule` returns YAML metadata, not full legal policy text or all Java semantics.

## Configuration

| Variable | Default / requirement |
|---|---|
| `INVESTIGATOR_POSTGRES_DSN` | Required; database login able to assume `governance_investigator` |
| `BEDROCK_MODEL_ID` | Required for investigate/once/worker; enabled Converse tool-use model/profile |
| `AWS_REGION` | Required for model calls |
| `INVESTIGATOR_RULE_IDS` | GOV-002,GOV-003,GOV-004,GOV-006,GOV-011,GOV-012 |
| `INVESTIGATOR_RULES_PATH` | Repository/container `config/rules.yml` |
| `INVESTIGATOR_POLL_SECONDS` | 5; allowed 1–300 |
| `INVESTIGATOR_MAX_STEPS` | 6; allowed 2–12 |
| `INVESTIGATOR_MAX_ATTEMPTS` | 3; allowed 1–5 |
| `INVESTIGATOR_TIMEOUT_SECONDS` | 120; allowed 10–300 |
| `INVESTIGATOR_MAX_TOKENS` | 1,500; allowed 256–4,096 |

The SDK uses its standard AWS credential chain. No credentials enter model messages or committed configuration. The selected AWS role needs `bedrock:InvokeModel` on the configured model/profile and any underlying model resources required by that profile; access depends on the account and region.

## Production database identity / EKS

A database administrator applies migrations 001 (if needed) and 002, then provisions a dedicated login and grants membership:

```sql
CREATE ROLE governance_agent_login LOGIN;
-- Set its password with the organization's secret-management workflow (not in this file).
GRANT governance_investigator TO governance_agent_login;
```

Set the runtime DSN to this dedicated login. Do not grant source-table write privileges. The Docker image runs as UID 10001. `deploy/k8s/investigator.yaml` provides an optional one-replica deployment with a **dedicated** service account and database Secret reference. Replace its image/region/model/role placeholders, provision the scoped IAM role and Secret, and apply the migration before deployment. This change does not apply Terraform, create AWS resources, or deploy the agent into your AWS account.

## Tests

Unit tests use a scripted model and botocore Stubber; integration tests use real PostgreSQL with scripted model responses. No test calls AWS or consumes production events.

```bash
uv pip install --python .venv/bin/python -r services/investigator-agent/requirements-dev.txt
.venv/bin/ruff check services/investigator-agent
.venv/bin/ruff format --check services/investigator-agent
cd services/investigator-agent
../../.venv/bin/pytest -q
```

For database tests, set `TEST_POSTGRES_DSN` to an **isolated** database whose name ends in `_test`. The fixture clears that database's event/finding/investigator tables. Without this variable, database tests are explicitly skipped. The GitHub Actions workflow supplies an isolated PostgreSQL service and runs the full suite plus the Docker build and Compose validation.

References: [Bedrock client-side tool use](https://docs.aws.amazon.com/bedrock/latest/userguide/tool-use-client-side.html), [Converse API](https://docs.aws.amazon.com/boto3/latest/reference/services/bedrock-runtime/client/converse.html).
