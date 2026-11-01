from __future__ import annotations

import json
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError
import psycopg
from psycopg.types.json import Jsonb


RAW_TOPICS = [
    "governance.keycloak.v1",
    "governance.kubernetes.v1",
    "governance.pipeline.v1",
    "governance.model-registry.v1",
    "governance.llm-gateway.v1",
]
FINDING_TOPIC = "governance.findings.scored.v1"
running = True


def kafka_config() -> dict:
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    }
    if config["security.protocol"] != "PLAINTEXT":
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        region = os.environ["AWS_REGION"]

        def oauth_cb(_):
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
            return token, expiry_ms / 1000

        config.update({"sasl.mechanism": "OAUTHBEARER", "oauth_cb": oauth_cb})
    return config


def connect_database():
    dsn = os.getenv("POSTGRES_DSN", "postgresql://governance:governance@localhost:5432/governance")
    while running:
        try:
            return psycopg.connect(dsn, autocommit=False)
        except psycopg.OperationalError:
            time.sleep(2)
    raise RuntimeError("Stopping before database became available")


def write_raw(connection, event: dict) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_events
              (event_id, schema_version, producer, event_type, event_time, ingested_at,
               environment, correlation_id, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event["event_id"], event["schema_version"], event["producer"], event["event_type"],
                event["event_time"], event["ingested_at"], event["environment"],
                event["correlation_id"], Jsonb(event),
            ),
        )


def write_finding(connection, finding: dict) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO compliance_findings
              (finding_id, rule_id, title, producer, source_event_id, source_event_time,
               detected_at, window_start, window_end, severity, severity_label, frequency,
               frequency_band, baseline_risk, isolation_forest_risk, risk_delta,
               recommended_risk, model_status, frameworks, evidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (finding_id) DO UPDATE SET
              isolation_forest_risk = EXCLUDED.isolation_forest_risk,
              risk_delta = EXCLUDED.risk_delta,
              recommended_risk = EXCLUDED.recommended_risk,
              model_status = EXCLUDED.model_status,
              persisted_at = now()
            """,
            (
                finding["finding_id"], finding["rule_id"], finding["title"], finding["producer"],
                finding["source_event_id"], finding["source_event_time"], finding["detected_at"],
                finding.get("window_start"), finding.get("window_end"), finding["severity"],
                finding["severity_label"], finding["frequency"], finding["frequency_band"],
                finding["baseline_risk"], finding.get("isolation_forest_risk"), finding.get("risk_delta"),
                finding.get("recommended_risk", finding["baseline_risk"]), finding.get("model_status"),
                Jsonb(finding["frameworks"]), Jsonb(finding["evidence"]),
            ),
        )


def stop(*_):
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer = Consumer({
        **kafka_config(),
        "group.id": os.getenv("KAFKA_GROUP_ID", "governance-postgres-sink-v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(RAW_TOPICS + [FINDING_TOPIC])
    connection = connect_database()
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    raise RuntimeError(message.error())
                continue
            payload = json.loads(message.value())
            try:
                if message.topic() == FINDING_TOPIC:
                    write_finding(connection, payload)
                else:
                    write_raw(connection, payload)
                connection.commit()
                consumer.commit(message=message, asynchronous=False)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                connection.close()
                connection = connect_database()
    finally:
        connection.close()
        consumer.close()


if __name__ == "__main__":
    main()
