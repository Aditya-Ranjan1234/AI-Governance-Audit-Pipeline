"""Seed and measure a local two-agent regression corpus. Never calls a model itself."""

import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from html import escape
import json
from pathlib import Path
import subprocess
import sys
import uuid

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services/investigator-agent"))
from review_store import REVIEWER_VERSION  # noqa: E402


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def seed(args):
    old = json.loads((ROOT / "docs/e2e-results-2026-11-01.json").read_text())
    notes = json.loads((ROOT / "evaluation/e2e-review-notes-2026-11-01.json").read_text())["notes"]
    bad = {row["finding_id"]: row["note"] for row in notes}
    selected = [deepcopy(row) for row in old["reports"] if row["finding_id"] in bad]
    cases = [{"source_run_id": row["run_id"], "finding_id": row["finding_id"], "kind": "known_problem", "expected_note": bad[row["finding_id"]]} for row in selected]
    findings = {row["finding_id"]: deepcopy(row) for row in old["findings"]}
    facts = {
        "GOV-002": "The source records ROLE_GRANT with privileged_role=true and change_approved=false.",
        "GOV-003": "The source records LOGIN_SUCCESS with privileged_identity=true and mfa_used=false: successful authentication without MFA.",
        "GOV-004": "The source records a CLUSTER_ROLE_BINDING with cluster_role=cluster-admin and change_approved=false.",
        "GOV-006": "The source records POD_CREATE with privileged=true and image_approved=true. Privilege alone satisfies GOV-006.",
    }
    for rule, text in facts.items():
        original = next(row for row in old["reports"] if row["rule_id"] == rule)
        row = deepcopy(original)
        fid = str(uuid.uuid5(uuid.NAMESPACE_URL, "reviewer-clean-finding:" + rule))
        row.update(finding_id=fid, job_id=str(uuid.uuid5(uuid.NAMESPACE_URL, "reviewer-clean-job:" + rule)),
                   run_id=str(uuid.uuid5(uuid.NAMESPACE_URL, "reviewer-clean-run:" + rule)))
        finding = row["evidence"].pop("finding:" + original["finding_id"])
        finding["finding_id"] = fid
        row["evidence"]["finding:" + fid] = finding
        source_ref = "event:" + finding["source_event_id"]
        source = row["evidence"][source_ref]
        row["report"] = {
            "summary": {"text": text, "evidence_refs": [source_ref]},
            "observations": [{"text": "The stated attributes are recorded on this source event.", "evidence_refs": [source_ref]}],
            "timeline": [{"event_ref": source_ref, "event_time": source["event_time"], "description": "Source event recorded."}],
            "missing_evidence": ["This bounded snapshot does not establish session relationships or the existence of external approval records."],
            "recommended_next_steps": ["Have the owner review the recorded event and applicable policy."],
            "assessment": "evidence_review_complete",
        }
        if rule == "GOV-006":
            row["report"]["summary"]["evidence_refs"] += [ref for ref in row["evidence"] if ref.startswith("rule:")]
        findings[fid] = dict(findings[original["finding_id"]], finding_id=fid)
        selected.append(row)
        cases.append({"source_run_id": row["run_id"], "finding_id": fid, "kind": "clean_control", "expected_note": "Grounded factual control; no session, causal or global-absence claim."})
    with psycopg.connect(args.dsn, autocommit=True) as c:
        for name in ("001_schema.sql", "002_investigator.sql", "003_reviewer.sql"):
            c.execute((ROOT / "db" / name).read_text())
    with psycopg.connect(args.dsn) as c:
        for raw in old["source_events"]:
            c.execute("""INSERT INTO public.raw_events(event_id,schema_version,producer,event_type,event_time,ingested_at,environment,correlation_id,payload)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (raw["event_id"],raw["schema_version"],raw["producer"],raw["event_type"],raw["event_time"],raw["ingested_at"],raw["environment"],raw["correlation_id"],Jsonb(raw)))
        for row in selected:
            f = findings[row["finding_id"]]
            source = row["evidence"]["event:" + f["source_event_id"]]
            c.execute("""INSERT INTO public.compliance_findings
                (finding_id,rule_id,title,producer,source_event_id,source_event_time,detected_at,severity,severity_label,frequency,frequency_band,baseline_risk,recommended_risk,frameworks,evidence)
                VALUES(%s,%s,%s,%s,%s,%s,%s,4,%s,1,1,%s,%s,%s,'{}') ON CONFLICT DO NOTHING""",
                (f["finding_id"],f["rule_id"],f["title"],f["producer"],f["source_event_id"],source["event_time"],source["event_time"],f["severity_label"],f["baseline_risk"],f["recommended_risk"],Jsonb(f["frameworks"])))
            c.execute("""INSERT INTO investigator.jobs(job_id,finding_id,agent_version,policy_hash,model_id,status,attempts)
                VALUES(%s,%s,%s,%s,%s,'succeeded',1) ON CONFLICT DO NOTHING""",
                (row["job_id"],row["finding_id"],row["agent_version"],row["policy_hash"],row["model_id"]))
            c.execute("""INSERT INTO investigator.runs(run_id,job_id,attempt,status,report,evidence)
                VALUES(%s,%s,1,'succeeded',%s,%s) ON CONFLICT DO NOTHING""",
                (row["run_id"],row["job_id"],Jsonb(row["report"]),Jsonb(row["evidence"])))
        preserved = c.execute("SELECT run_id,report,evidence FROM investigator.runs ORDER BY run_id").fetchall()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = {"cases": cases, "original_snapshot_hash": digest(preserved), "scope": "Eight prior saved reports plus four constructed clean controls; isolated local database."}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"seeded_reports": len(cases), "known_problems": len(bad), "clean_controls": 4}))


def collect(args):
    data = json.loads((args.output / "manifest.json").read_text())
    verification = args.output / "verification.json"
    if verification.exists():
        data["verification"] = json.loads(verification.read_text())
    inspection = ROOT / "evaluation/reviewer-inspection-2026-11-02.json"
    if inspection.exists():
        data["additional_evidence_inspection"] = json.loads(inspection.read_text())
    roots = [row["source_run_id"] for row in data["cases"]]
    with psycopg.connect(args.dsn, row_factory=dict_row) as c:
        for table, key in (("reviewer.jobs", "review_jobs"), ("investigator.revision_jobs", "revision_jobs"), ("reviewer.latest_results", "results")):
            data[key] = c.execute(f"SELECT * FROM {table} WHERE source_run_id=ANY(%s::uuid[])", (roots,)).fetchall()
        data["reviews"] = c.execute("""SELECT j.source_run_id,j.revision_run_id,r.* FROM reviewer.runs r
            JOIN reviewer.jobs j USING(job_id) WHERE j.source_run_id=ANY(%s::uuid[]) ORDER BY r.finished_at""", (roots,)).fetchall()
        data["revisions"] = c.execute("""SELECT j.source_run_id,j.review_run_id,r.* FROM investigator.revision_runs r
            JOIN investigator.revision_jobs j USING(job_id) WHERE j.source_run_id=ANY(%s::uuid[]) ORDER BY r.finished_at""", (roots,)).fetchall()
        data["audit"] = c.execute("""SELECT 'reviewer' AS agent,a.* FROM reviewer.audit_events a JOIN reviewer.jobs j USING(job_id)
            WHERE j.source_run_id=ANY(%s::uuid[]) UNION ALL
            SELECT 'investigator' AS agent,a.* FROM investigator.revision_audit_events a JOIN investigator.revision_jobs j USING(job_id)
            WHERE j.source_run_id=ANY(%s::uuid[]) ORDER BY created_at""", (roots,roots)).fetchall()
        data["originals"] = c.execute("SELECT run_id,report,evidence FROM investigator.runs ORDER BY run_id").fetchall()
        preserved = [(row["run_id"],row["report"],row["evidence"]) for row in data["originals"]]
    data = json.loads(json.dumps(data, default=str))
    data["previous_configurations"] = {key: data[key] for key in ("review_jobs", "revision_jobs", "results", "reviews", "revisions")}
    data["review_jobs"] = [job for job in data["review_jobs"] if job["worker_version"] == REVIEWER_VERSION]
    job_ids = {job["job_id"] for job in data["review_jobs"]}
    data["reviews"] = [row for row in data["reviews"] if row["job_id"] in job_ids]
    review_ids = {row["run_id"] for row in data["reviews"]}
    data["revision_jobs"] = [job for job in data["revision_jobs"] if job["review_run_id"] in review_ids]
    revision_job_ids = {job["job_id"] for job in data["revision_jobs"]}
    data["revisions"] = [row for row in data["revisions"] if row["job_id"] in revision_job_ids]
    data["results"] = [row for row in data["results"] if row["worker_version"] == REVIEWER_VERSION]
    data["reviewer_version"] = REVIEWER_VERSION
    data["originals_preserved"] = digest(preserved) == data["original_snapshot_hash"]
    initial = {row["source_run_id"]: row["payload"] for row in data["reviews"] if not row["revision_run_id"] and row["status"] == "succeeded"}
    data["quality"] = {
        "known_problems_flagged": sum(initial.get(row["source_run_id"], {}).get("effective_verdict") in ("revision_required", "needs_human_review") for row in data["cases"] if row["kind"] == "known_problem"),
        "clean_controls_passed": sum(initial.get(row["source_run_id"], {}).get("effective_verdict") == "review_passed" for row in data["cases"] if row["kind"] == "clean_control"),
    }
    jobs = data["review_jobs"] + data["revision_jobs"]
    final = {row["source_run_id"]: row for row in data["results"]}
    data["execution_complete"] = len(initial) == len(data["cases"]) and len(final) == len(data["cases"]) and all(job["status"] == "succeeded" for job in jobs) and all(row["verdict"] != "revision_required" for row in final.values())
    data["final_verdicts"] = dict(Counter(row["verdict"] for row in final.values()))
    data["job_statuses"] = dict(Counter(job["status"] for job in jobs))
    data["quality_exceptions"] = [
        dict(row, initial_verdict=initial[row["source_run_id"]]["effective_verdict"])
        for row in data["cases"] if row["source_run_id"] in initial
        and ((row["kind"] == "known_problem" and initial[row["source_run_id"]]["effective_verdict"] == "review_passed")
             or (row["kind"] == "clean_control" and initial[row["source_run_id"]]["effective_verdict"] != "review_passed"))
    ]
    data["revision_bound_passed"] = all(sum(job["source_run_id"] == row["source_run_id"] for job in data["revision_jobs"]) <= 1 for row in data["cases"])
    data["generated_at"] = datetime.now(timezone.utc).isoformat()
    data["implementation_commit"] = subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
    (args.output / "results.json").write_text(json.dumps(data,indent=2)+"\n")
    return data


def render(data):
    def e(value):
        return escape(str(value))
    original = {row["run_id"]: row for row in data["originals"]}
    final = {row["source_run_id"]: row for row in data["results"]}
    cards = []
    exceptions = ''.join(f'<li><b>{"Missed known problem" if row["kind"] == "known_problem" else "Clean control challenged"}</b>: <code>{e(row["source_run_id"])}</code> — {e(row["expected_note"])}</li>' for row in data["quality_exceptions"])
    outcomes = ', '.join(f'{count} {verdict.replace("_", " ")}' for verdict, count in sorted(data["final_verdicts"].items())) or 'Pending'
    job_states = ', '.join(f'{count} {state}' for state, count in sorted(data["job_statuses"].items())) or 'Pending'
    verification = '<details><summary>Test, container and runtime permission checks</summary><pre>' + e(json.dumps(data.get("verification", {}), indent=2)) + '</pre></details>'
    inspected = data.get("additional_evidence_inspection", {})
    inspection = '<p class="note">' + e(inspected.get("conclusion", "")) + '</p><details><summary>Additional evidence inspection</summary><pre>' + e(json.dumps(inspected, indent=2)) + '</pre></details>' if inspected else ''
    for case in data["cases"]:
        root = case["source_run_id"]
        result = final.get(root, {})
        reviews = [row for row in data["reviews"] if row["source_run_id"] == root]
        revisions = [row for row in data["revisions"] if row["source_run_id"] == root]
        body = {"expected_review_note":case["expected_note"],"additional_inspection":[note["note"] for note in inspected.get("notes", []) if note["source_run_id"] == root],"original_report":original[root]["report"],"reviews":reviews,"revisions":revisions}
        cards.append(f'<details class="case"><summary>{e(case["kind"].replace("_"," "))} · {e(result.get("verdict","pending"))}<small>{e(root)}</small></summary><p><b>Original:</b> {e(original[root]["report"]["summary"]["text"])}</p><p><b>Final:</b> {e(result.get("report",{}).get("summary",{}).get("text","Pending"))}</p><details><summary>Feedback, evidence and revision trace</summary><pre>{e(json.dumps(body,indent=2))}</pre></details></details>')
    payload = json.dumps(data).replace('<','\\u003c')
    return f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Two-agent review results</title><style>
*{{box-sizing:border-box}}body{{margin:0;overflow-wrap:anywhere;background:#eef3f5;color:#15333e;font:16px/1.6 system-ui,sans-serif}}main{{max-width:1100px;margin:auto;padding:30px}}header{{background:#0d2935;color:white;border-radius:16px;padding:32px}}header p{{color:#d5e6ea}}h1{{font-size:clamp(28px,5vw,43px);line-height:1.15}}nav{{display:flex;gap:20px;flex-wrap:wrap;margin:22px 0}}a{{color:#087663}}section,.case{{background:white;border:1px solid #d3e1e4;border-radius:12px;padding:22px;margin:18px 0}}.stats{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}}.stat{{background:white;border-radius:12px;padding:20px;margin-top:20px}}.stat b{{display:block;font-size:30px}}.stat span,small{{font-size:13px;color:#536a73}}small{{display:block;overflow-wrap:anywhere}}summary{{cursor:pointer;font-weight:650}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#eaf1f3;padding:18px;max-height:600px;overflow:auto;font-size:12px}}.flow{{display:flex;gap:12px;flex-wrap:wrap}}.flow b{{padding:15px;background:#e5f3ee;border-radius:8px;flex:1;min-width:180px}}button,input{{font:inherit;padding:10px;border:1px solid #abc2c8;border-radius:6px}}input{{width:min(100%,500px)}}.note{{border-left:4px solid #d7a548;padding:14px;background:#fff8e7}}[hidden]{{display:none!important}}@media(max-width:650px){{main{{padding:15px}}header{{padding:22px}}.stats{{grid-template-columns:repeat(2,minmax(0,1fr))}}section,.case{{padding:16px}}}}
</style></head><body><main><header><p>INVESTIGATOR + EVIDENCE REVIEWER</p><h1>Two agents. One bounded correction loop.</h1><p>Local PostgreSQL and live Amazon Bedrock. The corpus contains eight previously flagged reports and four constructed clean controls.</p><p>Execution complete: <b>{e(data['execution_complete'])}</b> · Original snapshots preserved: <b>{e(data['originals_preserved'])}</b></p></header>
<nav><a href="#flow">Communication</a><a href="#results">Results</a><a href="#limits">Limits and evidence</a></nav><p>Final reviewer configuration: <code>{e(data['reviewer_version'])}</code>. Earlier trial records remain in the downloadable evidence.</p>
<div class="stats"><div class="stat"><b>{data['quality']['known_problems_flagged']} / 8</b><span>known problems flagged</span></div><div class="stat"><b>{data['quality']['clean_controls_passed']} / 4</b><span>clean controls passed</span></div><div class="stat"><b>{len(data['revision_jobs'])}</b><span>revision requests</span></div><div class="stat"><b>1</b><span>maximum revision per review chain</span></div></div>
<section id="flow"><h2>Exactly how they communicate</h2><div class="flow"><b>Investigator saves report + immutable evidence</b><b>Reviewer polls and claims a review job</b><b>Reviewer saves cited feedback + revision request</b><b>Investigator saves one new revision</b><b>Reviewer rechecks; unresolved issues go to human review</b></div><ol><li>Each review references the original <code>source_run_id</code>. Review jobs are deduplicated and leased.</li><li>The reviewer reads the saved report and evidence using bounded tools. Deterministic checks cannot be overridden by a passing model verdict.</li><li>A successful initial review requesting correction atomically inserts a request in <code>investigator.revision_jobs</code>.</li><li>The investigator reads that feedback, uses the original snapshot, and writes a new <code>investigator.revision_runs</code> record.</li><li>The reviewer discovers that result by <code>revision_run_id</code>. A second correction request becomes <code>needs_human_review</code>; database checks reject another revision.</li></ol></section>
<section id="results"><h2>Reports, feedback and revisions</h2><p><b>Latest outcomes:</b> {e(outcomes)}. <b>Workflow jobs:</b> {e(job_states)}.</p><input id="search" type="search" placeholder="Filter cases or verdicts" aria-label="Filter cases"><button id="download">Download full JSON</button><p class="note">A review verdict is advisory. Initial verdict counts measure whether the reviewer challenged each report, not whether it identified every expected issue or corrected it successfully. These counts describe this small regression corpus, not general factual accuracy or compliance.</p>{inspection}<h3>Initial review disagreements with the test labels</h3><ul>{exceptions or '<li>None observed among completed initial reviews.</li>'}</ul>{''.join(cards)}</section>
<section id="limits"><h2>Verification and practical limits</h2><p>Revision limit respected: <b>{e(data['revision_bound_passed'])}</b>. Worker errors and earlier attempts remain in the trace. The original investigations came from the earlier E2E run; the four controls were authored as test inputs. This run exercises the new review/revision exchange, not a new source-system or MSK/EKS deployment.</p><p>Both agents use restricted database roles and can write only their workflow records. No recommendations are executed. A model reviewer can miss errors or reject a correct statement; unresolved reviews remain visible.</p>{verification}<p>Implementation commit: <code>{e(data['implementation_commit'])}</code>. Generated UTC: {e(data['generated_at'])}.</p></section></main>
<script type="application/json" id="data">{payload}</script><script>document.getElementById('search').addEventListener('input',event=>{{const q=event.target.value.toLowerCase();document.querySelectorAll('.case').forEach(c=>c.hidden=!c.textContent.toLowerCase().includes(q))}});document.getElementById('download').addEventListener('click',()=>{{const u=URL.createObjectURL(new Blob([document.getElementById('data').textContent],{{type:'application/json'}}));const a=document.createElement('a');a.href=u;a.download='two-agent-results.json';a.click();setTimeout(()=>URL.revokeObjectURL(u),1000)}});</script></body></html>'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("seed","report"))
    parser.add_argument("--dsn",required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--html",type=Path)
    args = parser.parse_args()
    conn = conninfo_to_dict(args.dsn)
    if conn.get("host") not in ("127.0.0.1","localhost") or not conn.get("dbname","").endswith("_review_e2e"):
        raise ValueError("Evaluation is restricted to a local database ending in _review_e2e")
    if args.command == "seed":
        seed(args)
    else:
        data = collect(args)
        if args.html:
            args.html.parent.mkdir(parents=True,exist_ok=True)
            args.html.write_text(render(data))
            args.html.with_suffix('.json').write_text(json.dumps(data,indent=2)+'\n')
        print(json.dumps({key:data[key] for key in ('execution_complete','originals_preserved','revision_bound_passed','quality')}))


if __name__ == "__main__":
    main()
