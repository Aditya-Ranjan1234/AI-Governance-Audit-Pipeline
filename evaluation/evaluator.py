from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import time
from urllib.request import Request, urlopen
import uuid

from confluent_kafka import Producer
from jsonschema import Draft202012Validator, FormatChecker
import numpy as np
import psycopg
from psycopg.types.json import Jsonb


TOPICS = {
    "KEYCLOAK": "governance.keycloak.v1",
    "KUBERNETES": "governance.kubernetes.v1",
    "PIPELINE": "governance.pipeline.v1",
    "MODEL_REGISTRY": "governance.model-registry.v1",
    "LLM_GATEWAY": "governance.llm-gateway.v1",
}

ADAPTER_URLS = {
    "KEYCLOAK": ("KEYCLOAK_ADAPTER_URL", "http://keycloak-adapter:8080/events"),
    "KUBERNETES": ("KUBERNETES_ADAPTER_URL", "http://kubernetes-audit-adapter:8080/events"),
    "PIPELINE": ("PIPELINE_ADAPTER_URL", "http://airflow-openlineage-adapter:8080/events"),
    "MODEL_REGISTRY": ("MODEL_REGISTRY_ADAPTER_URL", "http://model-registry-adapter:8080/events"),
    "LLM_GATEWAY": ("LLM_GATEWAY_ADAPTER_URL", "http://llm-gateway-adapter:8080/events"),
}


def native_payload(event):
    """Create source-shaped test input, preserving rule conditions and stable source IDs."""
    raw = {key: event[key] for key in (
        "event_time", "event_type", "actor", "subject", "action", "outcome",
        "environment", "correlation_id", "attributes", "labels",
    )}
    source_id = event["event_id"]
    producer = event["producer"]
    if producer == "KEYCLOAK":
        raw.update(id=source_id, type=event["event_type"], userId=event["actor"]["id"])
    elif producer == "KUBERNETES":
        raw.update(auditID=source_id, verb="create", user={"username": event["actor"]["id"]},
                   objectRef={"resource": "resource", "name": event["subject"]["id"]})
    elif producer == "PIPELINE":
        raw.update(run={"runId": source_id}, job={"name": event["subject"]["id"], "namespace": "e2e"})
    elif producer == "MODEL_REGISTRY":
        raw.update(id=source_id, model={"name": event["subject"].get("model_name"),
                                       "version": event["subject"].get("model_version")})
    else:
        raw.update(request_id=source_id, user_id=event["actor"]["id"],
                   prompt="E2E_RAW_PROMPT_DO_NOT_STORE",
                   model={"name": "e2e-endpoint", "version": "1"})
    return raw


def adapter_event_id(producer, source_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"governance:{producer}:{source_id}"))


def envelope(producer, event_type, attributes, at, *, actor="evaluation-user", subject=None, environment="prod"):
    event_id = str(uuid.uuid4())
    model_name, model_version = (subject or {}).get("model_name"), (subject or {}).get("model_version")
    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "event_time": at.isoformat().replace("+00:00", "Z"),
        "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "producer": producer,
        "event_type": event_type,
        "actor": {"id": actor, "type": "USER", "roles": [], "ip": "192.0.2.10", "namespace": "team-a"},
        "subject": {
            "type": (subject or {}).get("type", "RESOURCE"),
            "id": (subject or {}).get("id", f"fixture-{event_id}"),
            "namespace": (subject or {}).get("namespace"),
            "model_name": model_name,
            "model_version": model_version,
            "data_classification": (subject or {}).get("data_classification"),
        },
        "action": event_type.lower(),
        "outcome": "FAILURE" if event_type == "LOGIN_FAILURE" else "SUCCESS",
        "environment": environment,
        "correlation_id": f"evaluation-{event_id}",
        "trace_id": None,
        "attributes": attributes,
        "labels": {"synthetic": "true"},
    }


def scenarios(repeat, base, run_tag="fixture"):
    events = []
    expected = []

    def add(event, *rules):
        events.append(event)
        expected.extend((event["event_id"], rule) for rule in rules)

    user = f"burst-user-{run_tag}-{repeat}"
    for index in range(5):
        login = envelope("KEYCLOAK", "LOGIN_FAILURE", {}, base + timedelta(seconds=index), actor=user)
        if index == 4:
            add(login, "GOV-001")
        else:
            add(login)
    add(envelope("KEYCLOAK", "ROLE_GRANT", {"privileged_role": True, "change_approved": False}, base + timedelta(seconds=6)), "GOV-002")
    add(envelope("KEYCLOAK", "LOGIN_SUCCESS", {"privileged_identity": True, "mfa_used": False}, base + timedelta(seconds=7)), "GOV-003")
    add(envelope("KUBERNETES", "CLUSTER_ROLE_BINDING", {"cluster_role": "cluster-admin", "change_approved": False}, base + timedelta(seconds=8)), "GOV-004")
    add(envelope("KUBERNETES", "SECRET_ACCESS", {"cross_namespace": True}, base + timedelta(seconds=9)), "GOV-005")
    add(envelope("KUBERNETES", "POD_CREATE", {"privileged": True, "image_approved": True}, base + timedelta(seconds=10)), "GOV-006")
    add(envelope("PIPELINE", "DATASET_READ", {"dataset_owner": ""}, base + timedelta(seconds=11)), "GOV-007")
    add(envelope("PIPELINE", "SCHEMA_CHANGE", {"version_bumped": False}, base + timedelta(seconds=12)), "GOV-008")
    add(envelope("PIPELINE", "DRIFT_EVALUATION", {"metric_value": 0.35, "threshold": 0.20, "remediation_status": "NOT_STARTED"}, base + timedelta(seconds=13)), "GOV-009")
    add(envelope("PIPELINE", "BIAS_EVALUATION", {"metric_value": 0.19, "threshold": 0.10, "approved_exception": False}, base + timedelta(seconds=14)), "GOV-010")
    model_version = f"{run_tag}-{repeat}"
    model = {"type": "MODEL", "id": f"credit-risk:{model_version}", "model_name": "credit-risk", "model_version": model_version}
    add(envelope("MODEL_REGISTRY", "MODEL_PROMOTED", {}, base + timedelta(seconds=15), subject=model), "GOV-011", "GOV-012")
    add(envelope("MODEL_REGISTRY", "ARTIFACT_REGISTERED", {"signature_verified": False, "sha256": ""}, base + timedelta(seconds=16), subject=model), "GOV-013")
    add(envelope("LLM_GATEWAY", "PROMPT_REQUEST", {"pii_detected": True, "pii_handling_allowed": False, "model_approved": True, "prompt_injection_score": 0.1}, base + timedelta(seconds=17)), "GOV-014")
    add(envelope("LLM_GATEWAY", "PROMPT_REQUEST", {"pii_detected": False, "model_approved": False, "prompt_injection_score": 0.1}, base + timedelta(seconds=18)), "GOV-015")
    add(envelope("LLM_GATEWAY", "PROMPT_REQUEST", {"pii_detected": False, "model_approved": True, "prompt_injection_score": 0.96}, base + timedelta(seconds=19)), "GOV-016")

    # Five benign controls exercise negative cases and contribute to precision measurement.
    add(envelope("KEYCLOAK", "LOGIN_SUCCESS", {"privileged_identity": True, "mfa_used": True}, base + timedelta(seconds=20)))
    add(envelope("KUBERNETES", "POD_CREATE", {"privileged": False, "image_approved": True}, base + timedelta(seconds=21)))
    add(envelope("PIPELINE", "SCHEMA_CHANGE", {"version_bumped": True}, base + timedelta(seconds=22)))
    add(envelope("MODEL_REGISTRY", "ARTIFACT_REGISTERED", {"signature_verified": True, "sha256": "abc123"}, base + timedelta(seconds=23), subject=model))
    add(envelope("LLM_GATEWAY", "PROMPT_REQUEST", {"pii_detected": False, "model_approved": True, "prompt_injection_score": 0.02}, base + timedelta(seconds=24)))
    return events, expected


def inject(args):
    with open("governance-event.schema.json") as schema_file:
        validator = Draft202012Validator(json.load(schema_file), format_checker=FormatChecker())
    via_adapters = args.via_adapters
    producer = None if via_adapters else Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"), "enable.idempotence": True, "acks": "all"})
    run_id = uuid.uuid4()
    all_events, all_expected = [], []
    # Keep each synthetic failed-login burst within one five-minute tumbling window.
    now = datetime.now(timezone.utc)
    base = now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
    for repeat in range(args.repeats):
        run_events, run_expected = scenarios(repeat + 1, base, str(run_id)[:8])
        all_events.extend(run_events)
        all_expected.extend(run_expected)
    for event in all_events:
        validator.validate(event)

    payloads = [native_payload(event) for event in all_events] if via_adapters else []
    if via_adapters:
        mapped = {event["event_id"]: adapter_event_id(event["producer"], event["event_id"]) for event in all_events}
        all_expected = [(mapped[event_id], rule) for event_id, rule in all_expected]
        for event in all_events:
            event["event_id"] = mapped[event["event_id"]]

    with psycopg.connect(os.environ["POSTGRES_DSN"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO evaluation_runs (run_id, expected_findings, injected_events) VALUES (%s, %s, %s)",
                (run_id, len(all_expected), len(all_events)),
            )
            cursor.executemany(
                "INSERT INTO evaluation_events (run_id, event_id) VALUES (%s, %s)",
                [(run_id, event["event_id"]) for event in all_events],
            )
            cursor.executemany(
                "INSERT INTO evaluation_ground_truth (run_id, event_id, expected_rule_id) VALUES (%s, %s, %s)",
                [(run_id, event_id, rule) for event_id, rule in all_expected],
            )

    delivery_errors = []

    def delivered(error, _message):
        if error is not None:
            delivery_errors.append(str(error))

    for index, event in enumerate(all_events):
        if via_adapters:
            variable, default = ADAPTER_URLS[event["producer"]]
            request = Request(os.getenv(variable, default), data=json.dumps(payloads[index]).encode(),
                              headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=20) as response:
                accepted = json.load(response)
            if accepted.get("accepted") != 1 or accepted.get("event_ids") != [event["event_id"]]:
                raise RuntimeError("Adapter acknowledgement did not match the expected event ID")
            if args.delay_ms > 0:
                time.sleep(args.delay_ms / 1000)
            continue
        key = event["actor"]["id"]
        if event["subject"].get("model_name") and event["subject"].get("model_version"):
            key = f'{event["subject"]["model_name"]}:{event["subject"]["model_version"]}'
        producer.produce(TOPICS[event["producer"]], key=key.encode(), value=json.dumps(event).encode(), on_delivery=delivered)
        producer.poll(0)
        if args.delay_ms > 0:
            time.sleep(args.delay_ms / 1000)
    if producer is not None and (producer.flush(30) != 0 or delivery_errors):
        raise RuntimeError("Kafka did not acknowledge every evaluation event")
    print(json.dumps({"run_id": str(run_id), "events": len(all_events), "expected_findings": len(all_expected),
                      "ingestion": "http_adapters" if via_adapters else "direct_kafka"}, indent=2))


def percentile(values, q):
    return round(float(np.percentile(np.asarray(values), q)), 2) if values else None


def report(args):
    dsn = os.environ["POSTGRES_DSN"]
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT run_id, expected_findings, injected_events FROM evaluation_runs
                WHERE (%s::uuid IS NULL OR run_id=%s::uuid) ORDER BY started_at DESC LIMIT 1""", (args.run_id, args.run_id))
            latest = cursor.fetchone()
            if latest is None:
                raise SystemExit("No evaluation run found. Run 'inject' first.")
            run_id, expected_count, injected_count = latest
        deadline = time.time() + args.wait
        while True:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT count(*) FROM compliance_findings f
                       JOIN evaluation_events e ON e.run_id=%s AND e.event_id=f.source_event_id""",
                    (run_id,),
                )
                observed = cursor.fetchone()[0]
                cursor.execute("""SELECT count(*) FROM raw_events r JOIN evaluation_events e
                    ON e.run_id=%s AND e.event_id=r.event_id""", (run_id,))
                persisted = cursor.fetchone()[0]
            if (observed >= expected_count and persisted >= injected_count) or time.time() >= deadline:
                break
            time.sleep(2)

        with connection.cursor() as cursor:
            cursor.execute("SELECT event_id::text, expected_rule_id FROM evaluation_ground_truth WHERE run_id=%s", (run_id,))
            expected = set(cursor.fetchall())
            cursor.execute(
                """SELECT DISTINCT f.source_event_id::text, f.rule_id
                   FROM compliance_findings f
                   WHERE f.source_event_id IN (SELECT event_id FROM evaluation_events WHERE run_id=%s)""",
                (run_id,),
            )
            actual = set(cursor.fetchall())
            cursor.execute(
                """SELECT EXTRACT(EPOCH FROM (f.persisted_at-r.ingested_at))*1000
                   FROM compliance_findings f JOIN raw_events r ON r.event_id=f.source_event_id
                   WHERE r.event_id IN (SELECT event_id FROM evaluation_events WHERE run_id=%s)""",
                (run_id,),
            )
            latencies = [max(0.0, float(row[0])) for row in cursor.fetchall()]
            cursor.execute(
                """SELECT count(*), EXTRACT(EPOCH FROM (max(persisted_at)-min(persisted_at)))
                   FROM raw_events WHERE event_id IN
                   (SELECT event_id FROM evaluation_events WHERE run_id=%s)""",
                (run_id,),
            )
            event_count, duration = cursor.fetchone()

            true_positive = len(expected & actual)
            false_positive = len(actual - expected)
            false_negative = len(expected - actual)
            precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0
            recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0
            result = {
                "run_id": str(run_id),
                "true_positive": true_positive,
                "false_positive": false_positive,
                "false_negative": false_negative,
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "latency_ms": {"p50": percentile(latencies, 50), "p95": percentile(latencies, 95), "p99": percentile(latencies, 99)},
                "throughput_events_per_second": round(event_count / max(float(duration or 0), 0.001), 2),
                "observed_findings": len(actual),
                "expected_findings": len(expected),
                "injected_events": injected_count,
                "persisted_events": event_count,
                "missing_findings": sorted(expected - actual),
                "unexpected_findings": sorted(actual - expected),
                "passed": false_positive == 0 and false_negative == 0 and event_count == injected_count,
            }
            cursor.execute(
                "UPDATE evaluation_runs SET completed_at=now(), results=%s WHERE run_id=%s",
                (Jsonb(result), run_id),
            )
        connection.commit()
    print(json.dumps(result, indent=2))
    if not result["passed"]:
        raise SystemExit(1)


def adapter_smoke(args):
    marker = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    samples = [
        (
            os.getenv("KEYCLOAK_ADAPTER_URL", "http://keycloak-adapter:8080/events"),
            {"id": f"kc-{marker}", "time": now, "type": "ROLE_MAPPING_CREATE", "userId": "smoke-user", "attributes": {"privileged_role": True, "change_approved": False}},
            "GOV-002",
        ),
        (
            os.getenv("KUBERNETES_ADAPTER_URL", "http://kubernetes-audit-adapter:8080/events"),
            {"auditID": f"ka-{marker}", "requestReceivedTimestamp": now, "verb": "create", "user": {"username": "smoke-admin"}, "objectRef": {"resource": "clusterrolebindings", "name": "smoke-binding"}, "requestObject": {"roleRef": {"name": "cluster-admin"}}},
            "GOV-004",
        ),
        (
            os.getenv("PIPELINE_ADAPTER_URL", "http://airflow-openlineage-adapter:8080/events"),
            {"eventTime": now, "eventType": "COMPLETE", "run": {"runId": f"run-{marker}", "facets": {"governance": {"event_type": "DRIFT_EVALUATION", "metric_value": 0.4, "threshold": 0.2, "remediation_status": "NOT_STARTED"}}}, "job": {"namespace": "airflow", "name": "smoke-drift"}},
            "GOV-009",
        ),
        (
            os.getenv("MODEL_REGISTRY_ADAPTER_URL", "http://model-registry-adapter:8080/events"),
            {"id": f"mr-{marker}", "event_time": now, "action": "artifact_created", "model": {"name": "smoke-model", "version": marker[:8]}, "attributes": {"signature_verified": False, "sha256": ""}},
            "GOV-013",
        ),
        (
            os.getenv("LLM_GATEWAY_ADAPTER_URL", "http://llm-gateway-adapter:8080/events"),
            {"request_id": f"llm-{marker}", "event_time": now, "user_id": "smoke-user", "prompt": "must not be persisted", "pii_detected": True, "pii_handling_allowed": False, "model_approved": True, "prompt_injection_score": 0.1, "model": {"name": "smoke-llm", "version": "1"}},
            "GOV-014",
        ),
    ]
    expected = set()
    for url, payload, rule_id in samples:
        request = Request(url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=15) as response:
            result = json.load(response)
        if result.get("accepted") != 1:
            raise RuntimeError(f"Adapter at {url} did not accept exactly one event: {result}")
        expected.add((result["event_ids"][0], rule_id))

    deadline = time.time() + args.wait
    actual = set()
    event_ids = [event_id for event_id, _ in expected]
    with psycopg.connect(os.environ["POSTGRES_DSN"]) as connection:
        while time.time() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT source_event_id::text, rule_id FROM compliance_findings WHERE source_event_id = ANY(%s::uuid[])",
                    (event_ids,),
                )
                actual = set(cursor.fetchall())
            if expected <= actual:
                break
            time.sleep(1)
    result = {
        "adapters_tested": 5,
        "expected_findings": len(expected),
        "observed_findings": len(expected & actual),
        "missing": sorted(expected - actual),
    }
    print(json.dumps(result, indent=2))
    if expected - actual:
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    inject_parser = subparsers.add_parser("inject")
    inject_parser.add_argument("--repeats", type=int, default=3)
    inject_parser.add_argument("--delay-ms", type=float, default=0, help="Delay between events for failure testing")
    inject_parser.add_argument("--via-adapters", action="store_true", help="Send every scenario through the five HTTP adapters")
    inject_parser.set_defaults(function=inject)
    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("--wait", type=int, default=90)
    report_parser.add_argument("--run-id", type=uuid.UUID, help="Measure this exact run instead of the latest run")
    report_parser.set_defaults(function=report)
    adapter_parser = subparsers.add_parser("adapter-smoke")
    adapter_parser.add_argument("--wait", type=int, default=90)
    adapter_parser.set_defaults(function=adapter_smoke)
    args = parser.parse_args()
    args.function(args)


if __name__ == "__main__":
    main()
