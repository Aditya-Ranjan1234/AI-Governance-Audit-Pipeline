from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
from threading import Lock, Thread
from typing import Callable
import uuid

from confluent_kafka import Producer
from jsonschema import Draft202012Validator, FormatChecker


Mapper = Callable[[dict], dict]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_time(value) -> str:
    if value is None:
        return _utc_now()
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace("+00:00", "Z")
    text = str(value).strip()
    if text.isdigit():
        return _iso_time(int(text))
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _kafka_config(client_id: str) -> dict:
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "security.protocol": protocol,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 20,
    }
    if protocol != "PLAINTEXT":
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        region = os.environ["AWS_REGION"]

        def oauth_cb(_):
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
            return token, expiry_ms / 1000

        config.update({"sasl.mechanism": "OAUTHBEARER", "oauth_cb": oauth_cb})
    return config


def build_governance_event(raw: dict, producer_name: str, mapper: Mapper) -> dict:
    mapped = mapper(raw)
    source_id = mapped.pop("source_id", None)
    if source_id:
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"governance:{producer_name}:{source_id}"))
    else:
        event_id = str(uuid.uuid4())
    event_time = _iso_time(mapped.get("event_time"))
    mapped["event_time"] = event_time
    mapped["outcome"] = str(mapped.get("outcome", "UNKNOWN")).upper()
    if mapped["outcome"] not in {"SUCCESS", "FAILURE", "UNKNOWN"}:
        mapped["outcome"] = "UNKNOWN"
    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "event_time": event_time,
        "ingested_at": _utc_now(),
        "producer": producer_name,
        **mapped,
    }


class Adapter:
    def __init__(self, producer_name: str, topic: str, mapper: Mapper):
        self.producer_name = producer_name
        self.topic = topic
        self.mapper = mapper
        self.producer = Producer(_kafka_config(f"{producer_name.lower()}-adapter"))
        schema_path = Path(os.getenv("GOVERNANCE_SCHEMA_PATH", "/app/governance-event.schema.json"))
        self.validator = Draft202012Validator(json.loads(schema_path.read_text()), format_checker=FormatChecker())
        self.accepted = 0
        self.rejected = 0
        self.lock = Lock()

    @staticmethod
    def expand(payload) -> list[dict]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            return payload["items"]
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return payload["events"]
        if isinstance(payload, dict):
            return [payload]
        raise ValueError("Request body must be an event object or event array")

    def ready(self) -> bool:
        try:
            # confluent-kafka dispatches the Python OAuth callback from poll().
            # Prime the callback before the blocking metadata lookup.
            for _ in range(10):
                self.producer.poll(0.1)
            self.producer.list_topics(topic=self.topic, timeout=2)
            return True
        except Exception:
            return False

    def _dead_letter(self, raw: dict, error: Exception) -> None:
        source_id = raw.get("id") or raw.get("event_id") or raw.get("request_id") or raw.get("auditID")
        fingerprint = sha256(json.dumps(raw, sort_keys=True, default=str).encode()).hexdigest()
        payload = {
            "producer": self.producer_name,
            "source_id": str(source_id) if source_id else None,
            "payload_fingerprint": fingerprint,
            "error": str(error)[:1000],
            "received_at": _utc_now(),
        }
        self.producer.produce("governance.dead-letter.v1", value=json.dumps(payload).encode())
        self.producer.flush(5)

    def publish(self, payload) -> list[str]:
        event_ids = []
        for raw in self.expand(payload):
            try:
                event = build_governance_event(raw, self.producer_name, self.mapper)
                self.validator.validate(event)
                subject = event["subject"]
                key = event["actor"]["id"]
                if subject.get("model_name") and subject.get("model_version"):
                    key = f'{subject["model_name"]}:{subject["model_version"]}'
                timestamp_ms = int(datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")).timestamp() * 1000)
                self.producer.produce(
                    self.topic,
                    key=key.encode(),
                    value=json.dumps(event, separators=(",", ":")).encode(),
                    timestamp=timestamp_ms,
                )
                event_ids.append(event["event_id"])
            except Exception as error:
                with self.lock:
                    self.rejected += 1
                self._dead_letter(raw, error)
                raise
        if self.producer.flush(10) != 0:
            raise RuntimeError("Kafka delivery timed out")
        with self.lock:
            self.accepted += len(event_ids)
        return event_ids


def serve(producer_name: str, topic: str, mapper: Mapper) -> None:
    adapter = Adapter(producer_name, os.getenv("KAFKA_TOPIC", topic), mapper)

    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, body: dict):
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):
            if self.path == "/healthz":
                self._json(HTTPStatus.OK, {"status": "ok", "producer": producer_name})
            elif self.path == "/readyz":
                ready = adapter.ready()
                self._json(HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE, {"ready": ready})
            elif self.path == "/metrics":
                body = (
                    f'governance_adapter_events_total{{producer="{producer_name}",result="accepted"}} {adapter.accepted}\n'
                    f'governance_adapter_events_total{{producer="{producer_name}",result="rejected"}} {adapter.rejected}\n'
                ).encode()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def do_POST(self):
            if self.path != "/events":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > int(os.getenv("MAX_REQUEST_BYTES", "1048576")):
                    raise ValueError("Request body is empty or exceeds the configured limit")
                payload = json.loads(self.rfile.read(length))
                event_ids = adapter.publish(payload)
                self._json(HTTPStatus.ACCEPTED, {"accepted": len(event_ids), "event_ids": event_ids})
            except Exception as error:
                self._json(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(error)[:1000]})

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), Handler)
    def stop(*_):
        Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.serve_forever()
