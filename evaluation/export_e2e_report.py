"""Export an offline, inspectable report from the isolated local E2E database."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
from html import escape
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]


def serializable(value):
    def convert(item):
        if isinstance(item, Decimal):
            return float(item)
        if isinstance(item, datetime):
            return item.isoformat()
        return str(item)
    return json.loads(json.dumps(value, default=convert))


def collect(args):
    if conninfo_to_dict(args.dsn).get("host") not in {"localhost", "127.0.0.1"}:
        raise ValueError("This exporter is restricted to the isolated local test database")
    sys.path.insert(0, str(ROOT / "services/investigator-agent"))
    from contracts import validate_report
    from settings import AGENT_VERSION, DEFAULT_RULES
    from store import load_catalog
    _, policy_hash = load_catalog(ROOT / "config/rules.yml")
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "implementation_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "final_model": args.model,
        "final_agent_version": AGENT_VERSION,
        "final_policy_hash": policy_hash,
        "region": "us-west-2",
        "agent_count": 1,
        "deployment": "Local Docker reference pipeline with live Amazon Bedrock inference",
        "dataset": "Source-shaped synthetic governance events; no live source-system connectors",
        "tableau": "Skipped; database views and this offline report were verified instead",
        "baseline": json.loads((args.results / "metrics.json").read_text()),
        "adapter_smoke": json.loads((args.results / "adapter-smoke.json").read_text()),
        "additional_checks": json.loads((args.results / "additional-checks.json").read_text()),
        "recoveries": [json.loads((args.results / name / "result.json").read_text())
                       for name in ("processor-recovery", "broker-recovery")],
        "verification": json.loads((args.results / "verification.json").read_text()),
    }
    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        data["findings"] = conn.execute("""SELECT finding_id,rule_id,title,producer,source_event_id,
            severity_label,baseline_risk,isolation_forest_risk,recommended_risk,model_status,frameworks
            FROM compliance_findings ORDER BY rule_id,finding_id""").fetchall()
        data["source_events"] = [row["payload"] for row in conn.execute("SELECT payload FROM raw_events ORDER BY event_time,event_id")]
        data["reports"] = conn.execute("SELECT * FROM investigator.latest_reports WHERE model_id=%s AND agent_version=%s AND policy_hash=%s ORDER BY rule_id,finding_id", (args.model, AGENT_VERSION, policy_hash)).fetchall()
        data["jobs"] = conn.execute("""SELECT job_id,finding_id,agent_version,policy_hash,model_id,status,attempts,error_code,
            created_at,updated_at FROM investigator.jobs ORDER BY created_at,job_id""").fetchall()
        data["failed_attempts"] = conn.execute("""SELECT j.model_id,j.agent_version,j.finding_id,r.attempt,r.status,r.error_code,r.finished_at
            FROM investigator.runs r JOIN investigator.jobs j USING(job_id) WHERE r.status!='succeeded' ORDER BY r.finished_at""").fetchall()
        data["audit"] = conn.execute("SELECT job_id,attempt,kind,details,created_at FROM investigator.audit_events ORDER BY created_at,audit_id").fetchall()
        data["usage"] = conn.execute("""SELECT j.model_id,count(*) AS model_responses,
            sum((a.details->>'input_tokens')::bigint) AS input_tokens,
            sum((a.details->>'output_tokens')::bigint) AS output_tokens
            FROM investigator.audit_events a JOIN investigator.jobs j USING(job_id)
            WHERE a.kind='model_response' GROUP BY j.model_id ORDER BY j.model_id""").fetchall()
        data["eligible_findings"] = conn.execute("SELECT count(*) AS n FROM compliance_findings WHERE rule_id=ANY(%s)", (list(DEFAULT_RULES),)).fetchone()["n"]
        data["tableau_views"] = {}
        for view in ("tableau_framework_status", "tableau_risk_trend", "tableau_incident_history", "tableau_pipeline_latency"):
            data["tableau_views"][view] = conn.execute(f"SELECT count(*) AS n FROM {view}").fetchone()["n"]
    data = serializable(data)
    for row in data["reports"]:
        validate_report(row["report"], row["evidence"])
    data["validated_reports"] = len(data["reports"])
    final_jobs = [row for row in data["jobs"] if row["model_id"] == args.model and row["agent_version"] == AGENT_VERSION and row["policy_hash"] == policy_hash]
    checks = data["additional_checks"]
    data["passed"] = (
        data["baseline"]["passed"] and all(run["metrics"]["passed"] for run in data["recoveries"])
        and not data["adapter_smoke"]["missing"] and checks["replay"]["passed"]
        and checks["raw_prompt_redaction"]["passed"] and checks["source_write_rejected"]
        and len(data["reports"]) == len(final_jobs) == data["eligible_findings"]
        and all(row["status"] == "succeeded" for row in final_jobs)
    )
    return data


def render(data):
    def e(value):
        return escape(str(value), quote=True)

    def pretty(value):
        return escape(json.dumps(value, indent=2, ensure_ascii=False))
    time_local = datetime.fromisoformat(data["generated_at"]).astimezone(ZoneInfo("Asia/Kolkata"))
    metrics = data["baseline"]
    final_jobs = [row for row in data["jobs"] if row["model_id"] == data["final_model"] and row["agent_version"] == data["final_agent_version"] and row["policy_hash"] == data["final_policy_hash"]]
    state = "PASS · final configuration" if data["passed"] else "IN PROGRESS / NEEDS ATTENTION"
    scenario_rows = "".join(
        f'<tr><td>{e(name)}</td><td>{m["persisted_events"]} / {m["injected_events"]}</td>'
        f'<td>{m["observed_findings"]} / {m["expected_findings"]}</td><td>{m["precision"]:.0%} / {m["recall"]:.0%}</td>'
        f'<td>{m["latency_ms"]["p50"]/1000:.2f} / {m["latency_ms"]["p95"]/1000:.2f} / {m["latency_ms"]["p99"]/1000:.2f} s</td></tr>'
        for name, m in [("Baseline · all scenarios through HTTP", metrics)] +
        [(run["fault"].capitalize() + " recovery · fault during injection", run["metrics"]) for run in data["recoveries"]]
    )
    grouped = {}
    for finding in data["findings"]:
        grouped.setdefault(finding["rule_id"], []).append(finding)
    rule_rows = "".join(
        f'<tr><td><code>{e(rule)}</code></td><td>{e(rows[0]["title"])}</td><td>{e(rows[0]["producer"])}</td>'
        f'<td>{len(rows)}</td><td>{max(row["recommended_risk"] for row in rows):.1f}</td></tr>'
        for rule, rows in sorted(grouped.items())
    )
    reports = []
    for row in data["reports"]:
        report = row["report"]
        observations = "".join(f'<li>{e(item["text"])}<small>{e(" · ".join(item["evidence_refs"]))}</small></li>' for item in report["observations"])
        timeline = "".join(f'<li><code>{e(item["event_time"])}</code> — {e(item["description"])}<small>{e(item["event_ref"])}</small></li>' for item in report["timeline"])
        next_steps = "".join(f'<li>{e(item)}</li>' for item in report["recommended_next_steps"])
        missing = "".join(f'<li>{e(item)}</li>' for item in report["missing_evidence"]) or '<li>None listed by the model.</li>'
        reports.append(f'''<details class="report" id="report-{e(row['finding_id'])}"><summary><span class="rule">{e(row['rule_id'])}</span>
            {e(row['title'])}<span class="assessment">{e(report['assessment'].replace('_',' '))}</span></summary>
            <div class="report-body"><p class="report-summary">{e(report['summary']['text'])}</p>
            <small>Summary evidence: {e(' · '.join(report['summary']['evidence_refs']))}</small>
            <h4>Observed evidence</h4><ul>{observations}</ul><h4>Timeline · recorded UTC timestamps</h4><ol>{timeline}</ol>
            <div class="columns"><div><h4>Missing evidence</h4><ul>{missing}</ul></div><div><h4>Suggested next steps · advisory</h4><ul>{next_steps}</ul></div></div>
            <p class="meta">Finding <code>{e(row['finding_id'])}</code><br>Job <code>{e(row['job_id'])}</code><br>Model <code>{e(row['model_id'])}</code></p>
            <details><summary>Inspect the complete saved report and evidence snapshot</summary><pre>{pretty(row)}</pre></details></div></details>''')
    usage_rows = "".join(f'<tr><td><code>{e(row["model_id"])}</code></td><td>{row["model_responses"]}</td><td>{row["input_tokens"]:,}</td><td>{row["output_tokens"]:,}</td></tr>' for row in data["usage"])
    view_rows = "".join(f'<tr><td><code>{e(name)}</code></td><td>{count}</td></tr>' for name, count in data["tableau_views"].items())
    rules_ready = sum(row["model_status"] == "ready" for row in data["findings"])
    payload = json.dumps(data, ensure_ascii=True).replace("<", "\\u003c")
    return f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Governance pipeline · End-to-end results</title><style>
:root{{--ink:#16313b;--muted:#546b75;--bg:#f1f5f5;--line:#dce5e6;--green:#076e5e;--navy:#0d2935;--gold:#e8b86c}}
*{{box-sizing:border-box}}html{{scroll-behavior:auto;scroll-padding-top:24px}}body{{margin:0;background:var(--bg);color:var(--ink);font:16px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
a{{color:#086c89}}button,input{{font:inherit}}button{{cursor:pointer;background:white;border:1px solid #b9cccf;border-radius:7px;padding:8px 14px;color:var(--ink)}}button:hover{{background:#e8f2f2}}a:focus-visible,button:focus-visible,input:focus-visible,summary:focus-visible{{outline:3px solid #e8b86c;outline-offset:3px}}
.layout{{display:grid;grid-template-columns:230px minmax(0,1fr);max-width:1600px;margin:auto}}aside{{position:sticky;top:0;height:100vh;padding:32px 24px;background:var(--navy);color:#d8e8ed}}aside strong{{display:block;font-size:19px;line-height:1.35;margin-bottom:28px}}nav a{{display:block;color:#c8dde4;text-decoration:none;padding:10px 0;border-bottom:1px solid #28404b;font-size:14px}}aside p{{font-size:12px;color:#a9c4ce;margin-top:30px}}
main{{padding:40px;min-width:0}}header{{background:var(--navy);color:white;padding:36px;border-radius:14px;position:relative}}.eyebrow{{color:#9bd6ce;text-transform:uppercase;letter-spacing:.14em;font-size:12px;font-weight:700}}h1{{font-size:clamp(30px,4vw,47px);line-height:1.15;max-width:780px;margin:12px 0 18px;letter-spacing:-.03em}}header p{{color:#d0e2e8;max-width:760px;margin-bottom:12px}}.badge{{display:inline-block;padding:5px 12px;background:#d9f4e9;color:#075747;border-radius:30px;font-size:13px;font-weight:700}}.date{{font-size:13px;color:#b5cdd6;margin-top:18px}}
.cards{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:22px 0}}.card{{background:white;border:1px solid var(--line);border-radius:10px;padding:20px}}.card strong{{font-size:31px;display:block;line-height:1.2}}.card span{{font-size:13px;color:var(--muted)}}section{{background:white;border:1px solid var(--line);border-radius:12px;padding:28px;margin:22px 0;scroll-margin-top:24px}}h2{{font-size:25px;letter-spacing:-.02em;margin:0 0 12px}}h3{{font-size:18px;margin-top:26px}}h4{{font-size:15px;margin:22px 0 8px}}p{{margin:10px 0}}.muted,.meta{{color:var(--muted);font-size:14px}}.notice{{border-left:4px solid var(--gold);padding:12px 18px;background:#fff8e9;border-radius:0 8px 8px 0;margin:18px 0;font-size:14px}}.good{{border-left-color:#1b8b72;background:#eef9f4}}.flow{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:24px 0;list-style:none;padding:0;counter-reset:flow}}.flow li{{border:1px solid #cee0e1;background:#f2f8f8;padding:16px;border-radius:9px;font-size:14px;counter-increment:flow}}.flow li::before{{content:counter(flow,decimal-leading-zero);display:block;color:#0b7a6a;font-size:12px;letter-spacing:.1em;font-weight:700}}.flow b{{display:block;font-size:15px;margin:6px 0}}.flow small{{font-size:12px}}.table-wrap{{overflow:auto}}table{{width:100%;border-collapse:collapse;font-size:14px}}th{{text-align:left;color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.03em;border-bottom:2px solid var(--line);padding:12px 10px}}td{{vertical-align:top;padding:12px 10px;border-bottom:1px solid var(--line)}}tr:last-child td{{border-bottom:0}}code{{font:12px/1.6 ui-monospace,SFMono-Regular,Consolas,monospace;overflow-wrap:anywhere}}pre{{background:#102e3a;color:#d7edf3;padding:18px;border-radius:8px;font:12px/1.6 ui-monospace,SFMono-Regular,Consolas,monospace;white-space:pre-wrap;overflow-wrap:anywhere;max-height:520px;overflow:auto}}small{{display:block;color:var(--muted);font-size:11px;overflow-wrap:anywhere}}.columns{{display:grid;grid-template-columns:1fr 1fr;gap:24px}}li{{margin:7px 0}}.toolbar{{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin:18px 0}}input{{flex:1;min-width:200px;padding:10px 13px;border:1px solid #adc3c8;border-radius:8px}}.report{{border:1px solid var(--line);border-radius:9px;margin:12px 0;overflow:hidden}}summary{{cursor:pointer;padding:15px;background:#f6f9f9;font-size:14px;font-weight:600;overflow-wrap:anywhere}}.rule{{display:inline-block;margin-right:12px;background:#dcefeb;color:#086450;padding:2px 8px;border-radius:5px;font:12px ui-monospace,monospace}}.assessment{{display:block;font-size:11px;font-weight:400;color:var(--muted);margin-left:22px}}.report-body{{padding:22px}}.report-summary{{font-size:17px;font-weight:500}}.steps{{padding-left:24px}}.steps li{{padding:6px 0 10px}}.steps b{{display:block}}footer{{font-size:12px;color:var(--muted);padding:16px 0 34px}}[hidden]{{display:none!important}}
@media(max-width:1000px){{.layout{{grid-template-columns:180px minmax(0,1fr)}}main{{padding:24px}}aside{{padding:26px 18px}}.cards,.flow{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}
@media(max-width:700px){{.layout{{display:block}}aside{{position:static;height:auto;padding:18px}}aside strong{{margin:0 0 12px;font-size:17px}}nav{{display:flex;gap:18px;overflow:auto}}nav a{{white-space:nowrap;padding:4px 0}}aside p{{display:none}}main{{padding:16px}}header{{padding:24px}}section{{padding:20px}}.cards{{gap:8px}}.card{{padding:15px}}.card strong{{font-size:26px}}.columns{{display:block}}.report-body{{padding:15px}}th,td{{padding:10px 7px}}}}
@media print{{aside,.toolbar,button{{display:none}}.layout{{display:block}}main{{padding:0}}section{{break-inside:avoid}}header{{background:#eee;color:#111}}header p,.eyebrow,.date{{color:#333}}}}
</style></head><body><div class="layout"><aside><strong>Governance<br>pipeline results</strong><nav aria-label="Report sections">
<a href="#overview">Overview</a><a href="#flow">Executed flow</a><a href="#measurements">Measurements</a><a href="#findings">Findings</a><a href="#reports">Agent reports</a><a href="#communication">Agent communication</a><a href="#verification">Verification &amp; limits</a><a href="#reproduce">Reproduce &amp; inspect</a></nav><p>Offline report<br>No server or Tableau required<br>Export time uses IST. Evidence timestamps retain UTC (Z or +00:00).</p></aside>
<main><header id="overview"><div class="eyebrow">Execution started · 01 November 2026</div><h1>From source events to<br>cited investigation reports.</h1><span class="badge">{e(state)}</span><p>{e(data['deployment'])}. Five source adapters, all 16 governance rules, risk scoring, durable storage, and one investigator.</p><div class="date">Exported {e(time_local.strftime('%d %B %Y · %I:%M %p IST'))}</div></header>
<div class="cards"><div class="card"><strong>{len(data['source_events'])}</strong><span>unique events persisted</span></div><div class="card"><strong>{len(data['findings'])}</strong><span>governance findings</span></div><div class="card"><strong>{len(data['reports'])} / {data['eligible_findings']}</strong><span>eligible findings investigated</span></div><div class="card"><strong>{metrics['latency_ms']['p95']/1000:.2f}s</strong><span>baseline p95 detection latency</span></div></div>
<section id="flow"><h2>The flow that actually ran</h2><p>Labelled, source-shaped synthetic events were posted to the five HTTP services. The existing native-format smoke cases also passed for every adapter. The broker and database ran locally; model inference used the real AWS Bedrock API in <code>us-west-2</code>.</p>
<ol class="flow"><li><b>Five HTTP adapters</b>Keycloak, Kubernetes, Airflow/OpenLineage, model registry, LLM gateway.</li><li><b>Kafka-compatible broker</b>Redpanda; five source topics and raw/scored finding topics.</li><li><b>Kafka Streams rules</b>All 16 rules; failed-login windows and model-promotion joins.</li><li><b>Risk scorer</b>Baseline plus Isolation Forest; {rules_ready} of {len(data['findings'])} findings recorded after warm-up.</li><li><b>PostgreSQL sink</b>{len(data['source_events'])} canonical events and {len(data['findings'])} findings; replay verified.</li><li><b>One investigator</b>Durable jobs; restricted source reads; bounded tools and model turns.</li><li><b>Live Bedrock</b>Final model: Amazon Nova 2 Lite. No scripted model in the final flow.</li><li><b>Saved evidence</b>{len(data['reports'])} cited reports, evidence snapshots, job history and audit records.</li></ol>
<div class="notice good">Tableau was skipped as permitted. All four reporting views contain data; the results and investigation reports are available directly in this file.</div></section>
<section id="measurements"><h2>Measured pipeline behavior</h2><div class="table-wrap"><table><thead><tr><th>Scenario</th><th>Events stored / sent</th><th>Findings / expected</th><th>Precision / recall</th><th>p50 / p95 / p99</th></tr></thead><tbody>{scenario_rows}</tbody></table></div>
<p>The separate native-adapter smoke test produced <b>5 / 5</b> expected findings. Together these runs account for all {len(data['source_events'])} unique events and {len(data['findings'])} findings. The replay check resubmitted an existing event and observed its downstream update without adding another finding row.</p>
<p class="muted">Latency starts at adapter normalization and ends at finding persistence. These are small functional and recovery tests, not a sustained-load benchmark. The processor replacement preserved data but raised p95 latency to {data['recoveries'][0]['metrics']['latency_ms']['p95']/1000:.2f} seconds. Broker restart tests a local single broker; managed MSK failover was not exercised.</p></section>
<section id="findings"><h2>All 16 rules were exercised</h2><div class="table-wrap"><table><thead><tr><th>Rule</th><th>Finding</th><th>Producer</th><th>Count</th><th>Max risk / 100</th></tr></thead><tbody>{rule_rows}</tbody></table></div></section>
<section id="reports"><h2>Read the agent's saved results</h2><p>Final model <code>{e(data['final_model'])}</code>; version <code>{e(data['final_agent_version'])}</code>. Every displayed report passed schema, evidence-reference, exact-timestamp and narrow domain-wording validation. This proves reference validity; it does not prove every natural-language statement is entailed by the evidence. Recommended actions remain advisory.</p><div class="toolbar"><input id="report-search" type="search" placeholder="Filter by rule, finding, or report text" aria-label="Filter investigation reports"><button id="expand">Expand all reports</button><button id="download">Download complete evidence JSON</button></div><p id="report-count" class="muted">{len(data['reports'])} reports</p>{''.join(reports) or '<p>Reports are still being processed.</p>'}</section>
<section id="communication"><h2>Exactly how the agent communicates</h2><p><b>There is one product agent. No second agent and no agent-to-agent messages were introduced.</b> The existing deterministic services already handle normalization, detection, scoring and storage. The live run showed that provider compatibility and recovery needed attention first.</p>
<ol class="steps"><li><b>Pipeline → PostgreSQL</b>The sink writes <code>compliance_findings</code> and <code>raw_events</code>. The investigator reads these records; it does not join a Kafka consumer group.</li><li><b>Database → investigator</b>Polling discovers the six configured rule IDs. A unique key over finding ID, agent version, policy hash and model ID prevents duplicate jobs. <code>FOR UPDATE SKIP LOCKED</code> claims a job with a lease token.</li><li><b>Investigator → Bedrock</b>HTTPS <code>Converse</code> sends the finding, system instructions, conversation messages and four tool schemas. AWS requests use credentials; credentials are never part of model messages.</li><li><b>Bedrock → investigator tools</b>The response contains a <code>toolUseId</code>, an allowlisted tool name and JSON input. The worker executes <code>get_source_event</code>, <code>get_related_events</code> or <code>get_rule</code> using fixed parameterized queries. Read-tool inputs cannot select arbitrary IDs or SQL.</li><li><b>Tools → Bedrock</b>A <code>toolResult</code> echoes the same <code>toolUseId</code> and returns projected evidence with stable <code>event:</code>, <code>finding:</code> and <code>rule:</code> references. Related-event retrieval is limited to 20 events, ±15 minutes, matching environment and tenant marker, then model/version or actor.</li><li><b>Bedrock → report validator → PostgreSQL</b><code>submit_investigation</code> must occur separately after source and rule reads. Invalid reports receive safe corrective feedback. A valid report is committed to <code>investigator.runs</code>; job status and audit entries record its outcome. Lease fencing rejects stale completion.</li></ol>
<p class="muted">If a second agent is added later, the useful role is independent report review. A concrete future contract would be a durable review job keyed by <code>run_id</code>, reading the immutable report/evidence snapshot and writing a separate review verdict. That interface is a proposal, not a component built or tested in this run.</p></section>
<section id="verification"><h2>Verification and practical limits</h2><div class="columns"><div><h3>{data['verification']['automated_tests']} automated tests passed</h3><ul><li>{data['verification']['investigator_tests']} investigator tests, including real PostgreSQL integration.</li><li>7 Java rule/window/join tests.</li><li>5 adapter mapper tests, 3 native-ingestion tests and 3 risk-scoring tests.</li><li>Lint, formatting, Compose and Dockerfile checks.</li><li>Investigator image built; runtime UID 10001, read-only filesystem and short-lived AWS credentials.</li></ul></div><div><h3>Data and access checks</h3><ul><li>Raw prompt marker leaked rows: <b>{data['additional_checks']['raw_prompt_redaction']['leaked_rows']}</b>.</li><li>Replay: <b>{'passed' if data['additional_checks']['replay']['passed'] else 'failed'}</b>.</li><li>Actual source-table UPDATE rejected: <b>{data['additional_checks']['source_write_rejected']}</b>.</li><li>Database session: <code>{e(data['additional_checks']['database_identity']['session_user'])}</code>; effective role: <code>governance_investigator</code>.</li><li>Final model jobs succeeded: <b>{sum(row['status']=='succeeded' for row in final_jobs)} / {len(final_jobs)}</b>.</li></ul></div></div>
<div class="notice"><b>Initial model trials are preserved.</b> Nova Lite v1 produced invalid tool sequences and report-submission errors. Claude Haiku tool requests were blocked by an AWS Marketplace payment-instrument error; no billing settings were changed. The final configuration uses Amazon Nova 2 Lite with agent v1.3. Earlier Nova 2 passes exposed rule-wording mistakes and invalid timeline references. The final pass uses clarified rule metadata, projected evidence and an event-only timeline schema. Safe validation feedback, bounded model-error retries, and a stop condition for systemic configuration failures were added during this run.</div>
<details><summary>Inspect failed/retried attempts from all model trials</summary><pre>{pretty(data['failed_attempts'])}</pre></details>
<h3>Recorded model usage</h3><div class="table-wrap"><table><thead><tr><th>Model / profile</th><th>Responses</th><th>Input tokens</th><th>Output tokens</th></tr></thead><tbody>{usage_rows}</tbody></table></div><p class="muted">Usage above comes from persisted agent audit entries. Separate connectivity and diagnostic probes are excluded; this is not an AWS billing total.</p>
<h3>Tableau-ready views</h3><div class="table-wrap"><table><thead><tr><th>View</th><th>Rows</th></tr></thead><tbody>{view_rows}</tbody></table></div><p class="muted">No new EKS, MSK or RDS deployment was performed. The existing EKS check did not succeed. The verified scope is the local reference stack plus live Bedrock. Source systems were represented by synthetic requests.</p></section>
<section id="reproduce"><h2>Reproduce and inspect</h2><p>Implementation commit: <code>{e(data['implementation_commit'])}</code>. Branch: <code>codex/e2e-validation</code>.</p><pre>docker compose -p philstock-e2e-20261101 -f docker-compose.yml -f evaluation/compose.e2e.yml up -d --no-build --pull never
docker compose -p philstock-e2e-20261101 -f docker-compose.yml -f evaluation/compose.e2e.yml --profile evaluation run --rm --no-deps evaluator inject --via-adapters --repeats 3
docker compose -p philstock-e2e-20261101 -f docker-compose.yml -f evaluation/compose.e2e.yml --profile evaluation run --rm --no-deps evaluator report --run-id RUN_UUID</pre>
<p>Commands assume the dependency images and the current Java JAR are available. Detailed configuration and offline-build instructions are in <code>docs/investigator-agent.md</code>; execution notes are in <code>docs/e2e-run.md</code>. Raw local artifacts and the database backup are under <code>evaluation/results/e2e-2026-11-01/</code>.</p><details><summary>Run IDs and exact metrics</summary><pre>{pretty({'baseline':data['baseline'],'adapter_smoke':data['adapter_smoke'],'recoveries':data['recoveries'],'additional_checks':data['additional_checks']})}</pre></details></section>
<footer>Generated from persisted records and measured test results. Model text is displayed as escaped text. This report contains no AWS credentials or database passwords.</footer></main></div>
<script id="evidence-data" type="application/json">{payload}</script><script>
const cards=Array.from(document.querySelectorAll('.report'));const search=document.getElementById('report-search');
search.addEventListener('input',()=>{{const q=search.value.toLowerCase();let n=0;cards.forEach(card=>{{card.hidden=!card.textContent.toLowerCase().includes(q);if(!card.hidden)n++}});document.getElementById('report-count').textContent=n+' of '+cards.length+' reports';}});
let expanded=false;document.getElementById('expand').addEventListener('click',event=>{{expanded=!expanded;cards.filter(card=>!card.hidden).forEach(card=>card.open=expanded);event.target.textContent=expanded?'Collapse all reports':'Expand all reports';}});
document.getElementById('download').addEventListener('click',()=>{{const blob=new Blob([document.getElementById('evidence-data').textContent],{{type:'application/json'}});const url=URL.createObjectURL(blob);const a=document.createElement('a');a.href=url;a.download='governance-e2e-evidence-2026-11-01.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}});
function openTarget(){{const element=document.getElementById(location.hash.slice(1));if(element&&element.classList.contains('report'))element.open=true;}}window.addEventListener('hashchange',openTarget);openTarget();
</script></body></html>'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--model", default="us.amazon.nova-2-lite-v1:0")
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = collect(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(data))
    args.output.with_suffix(".json").write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps({"passed": data["passed"], "events": len(data["source_events"]), "findings": len(data["findings"]),
                      "reports": len(data["reports"]), "eligible_findings": data["eligible_findings"], "html": str(args.output)}))


if __name__ == "__main__":
    main()
