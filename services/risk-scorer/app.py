from __future__ import annotations

import json
import os
import signal

from confluent_kafka import Consumer, KafkaError, Producer

from scorer import scorer_from_environment


INPUT_TOPIC = "governance.findings.raw.v1"
OUTPUT_TOPIC = "governance.findings.scored.v1"
running = True


def kafka_config(client: str) -> dict:
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        "client.id": f"governance-risk-{client}",
    }
    if config["security.protocol"] != "PLAINTEXT":
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        region = os.environ["AWS_REGION"]

        def oauth_cb(_):
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
            return token, expiry_ms / 1000

        config.update({"sasl.mechanism": "OAUTHBEARER", "oauth_cb": oauth_cb})
    return config


def stop(*_):
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer = Consumer({
        **kafka_config("consumer"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "governance-risk-scorer-v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({**kafka_config("producer"), "enable.idempotence": True, "acks": "all"})
    scorer = scorer_from_environment()
    consumer.subscribe([INPUT_TOPIC])
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    raise RuntimeError(message.error())
                continue
            finding = json.loads(message.value())
            scored = scorer.score(finding)
            producer.produce(
                OUTPUT_TOPIC,
                key=scored["finding_id"].encode(),
                value=json.dumps(scored, separators=(",", ":")).encode(),
            )
            producer.flush(10)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
