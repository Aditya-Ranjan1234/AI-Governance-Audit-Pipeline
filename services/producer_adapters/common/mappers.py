from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _text(*values: Any, default: str = "") -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def _event_type(value: Any, default: str) -> str:
    return _text(value, default=default).upper().replace(".", "_").replace("-", "_").replace(" ", "_")


def _time(raw: dict) -> Any:
    return raw.get("event_time") or raw.get("eventTime") or raw.get("time") or raw.get("requestReceivedTimestamp")


def _environment(raw: dict) -> str:
    value = _text(raw.get("environment"), _dict(raw.get("context")).get("environment"), default="prod").lower()
    return value if value in {"dev", "test", "stage", "prod"} else "prod"


def _labels(raw: dict, source: str) -> dict[str, str]:
    supplied = _dict(raw.get("labels"))
    return {**{str(k): str(v) for k, v in supplied.items()}, "adapter": source}


def _subject(raw: dict, *, default_type: str, default_id: str, namespace: str | None = None, model_name: str | None = None, model_version: str | None = None) -> dict:
    supplied = _dict(raw.get("subject"))
    return {
        "type": _text(supplied.get("type"), default=default_type),
        "id": _text(supplied.get("id"), default_id, default="unknown"),
        "namespace": supplied.get("namespace", namespace),
        "model_name": supplied.get("model_name", model_name),
        "model_version": supplied.get("model_version", model_version),
        "data_classification": supplied.get("data_classification"),
    }


def keycloak(raw: dict) -> dict:
    auth = _dict(raw.get("authDetails"))
    details = _dict(raw.get("details"))
    supplied_actor = _dict(raw.get("actor"))
    source_type = _event_type(raw.get("event_type") or raw.get("type") or raw.get("operationType"), "KEYCLOAK_EVENT")
    aliases = {"LOGIN_ERROR": "LOGIN_FAILURE", "LOGIN": "LOGIN_SUCCESS", "ROLE_MAPPING_CREATE": "ROLE_GRANT"}
    event_type = aliases.get(source_type, source_type)
    actor_id = _text(supplied_actor.get("id"), auth.get("userId"), raw.get("userId"), details.get("username"), default="unknown")
    attributes = {**details, **_dict(raw.get("attributes"))}
    return {
        "source_id": raw.get("id") or raw.get("event_id"),
        "event_time": _time(raw),
        "event_type": event_type,
        "actor": {
            "id": actor_id,
            "type": supplied_actor.get("type", "USER"),
            "roles": supplied_actor.get("roles", raw.get("roles", [])),
            "ip": supplied_actor.get("ip", auth.get("ipAddress") or raw.get("ipAddress")),
            "namespace": supplied_actor.get("namespace"),
        },
        "subject": _subject(raw, default_type="IDENTITY", default_id=_text(raw.get("resourcePath"), actor_id)),
        "action": _text(raw.get("action"), event_type.lower()),
        "outcome": raw.get("outcome") or ("FAILURE" if raw.get("error") or event_type == "LOGIN_FAILURE" else "SUCCESS"),
        "environment": _environment(raw),
        "correlation_id": _text(raw.get("correlation_id"), raw.get("sessionId"), raw.get("id"), default=f"keycloak:{actor_id}"),
        "trace_id": raw.get("trace_id"),
        "attributes": attributes,
        "labels": _labels(raw, "keycloak"),
    }


def kubernetes_audit(raw: dict) -> dict:
    user = _dict(raw.get("user"))
    object_ref = _dict(raw.get("objectRef"))
    request_object = _dict(raw.get("requestObject"))
    metadata = _dict(request_object.get("metadata"))
    annotations = _dict(metadata.get("annotations"))
    supplied = _dict(raw.get("attributes"))
    verb = _text(raw.get("verb"), raw.get("action"), default="unknown").lower()
    resource = _text(object_ref.get("resource"), default="resource").lower()
    if raw.get("event_type"):
        event_type = _event_type(raw["event_type"], "KUBERNETES_AUDIT")
    elif verb in {"create", "update", "patch"} and resource == "clusterrolebindings":
        event_type = "CLUSTER_ROLE_BINDING"
    elif verb in {"get", "list", "watch"} and resource == "secrets":
        event_type = "SECRET_ACCESS"
    elif verb == "create" and resource == "pods":
        event_type = "POD_CREATE"
    else:
        event_type = _event_type(f"K8S_{verb}_{resource}", "KUBERNETES_AUDIT")

    spec = _dict(request_object.get("spec"))
    containers = list(spec.get("containers") or []) + list(spec.get("initContainers") or [])
    privileged = any(bool(_dict(_dict(c).get("securityContext")).get("privileged")) for c in containers)
    namespace = object_ref.get("namespace") or metadata.get("namespace")
    role_ref = _dict(request_object.get("roleRef"))
    attributes = {
        "cluster_role": _text(supplied.get("cluster_role"), role_ref.get("name"), object_ref.get("name")),
        "change_approved": bool(supplied.get("change_approved", annotations.get("governance.example/approved") == "true")),
        "cross_namespace": bool(supplied.get("cross_namespace", False)),
        "privileged": bool(supplied.get("privileged", privileged)),
        "image_approved": bool(supplied.get("image_approved", annotations.get("governance.example/image-approved") == "true")),
        **supplied,
    }
    actor_id = _text(_dict(raw.get("actor")).get("id"), user.get("username"), default="unknown")
    return {
        "source_id": raw.get("auditID") or raw.get("event_id"),
        "event_time": _time(raw) or raw.get("stageTimestamp"),
        "event_type": event_type,
        "actor": {
            "id": actor_id,
            "type": "SERVICE" if actor_id.startswith("system:serviceaccount:") else "USER",
            "roles": user.get("groups", []),
            "ip": (raw.get("sourceIPs") or [None])[0],
            "namespace": namespace,
        },
        "subject": _subject(raw, default_type=resource.upper(), default_id=_text(object_ref.get("name"), object_ref.get("uid"), default=resource), namespace=namespace),
        "action": verb,
        "outcome": "FAILURE" if int(_dict(raw.get("responseStatus")).get("code", 200)) >= 400 else "SUCCESS",
        "environment": _environment(raw),
        "correlation_id": _text(raw.get("correlation_id"), raw.get("auditID"), default=f"kubernetes:{actor_id}"),
        "trace_id": raw.get("trace_id"),
        "attributes": attributes,
        "labels": _labels(raw, "kubernetes-audit"),
    }


def airflow_openlineage(raw: dict) -> dict:
    run = _dict(raw.get("run"))
    job = _dict(raw.get("job"))
    run_facets = _dict(run.get("facets"))
    job_facets = _dict(job.get("facets"))
    governance = {**_dict(job_facets.get("governance")), **_dict(run_facets.get("governance")), **_dict(raw.get("attributes"))}
    event_type = _event_type(raw.get("event_type") or governance.pop("event_type", None) or raw.get("eventType"), "OPENLINEAGE_EVENT")
    job_name = _text(job.get("name"), raw.get("dag_id"), default="unknown-job")
    namespace = _text(job.get("namespace"), raw.get("namespace"), default="airflow")
    actor_id = _text(_dict(raw.get("actor")).get("id"), raw.get("service_account"), default="airflow")
    return {
        "source_id": run.get("runId") or raw.get("run_id") or raw.get("event_id"),
        "event_time": _time(raw),
        "event_type": event_type,
        "actor": {"id": actor_id, "type": "SERVICE", "roles": ["pipeline"], "ip": None, "namespace": namespace},
        "subject": _subject(raw, default_type="PIPELINE_JOB", default_id=job_name, namespace=namespace),
        "action": _text(raw.get("action"), event_type.lower()),
        "outcome": "FAILURE" if event_type in {"FAIL", "FAILED"} else "SUCCESS",
        "environment": _environment(raw),
        "correlation_id": _text(raw.get("correlation_id"), run.get("runId"), default=f"pipeline:{job_name}"),
        "trace_id": raw.get("trace_id"),
        "attributes": governance,
        "labels": _labels(raw, "airflow-openlineage"),
    }


def model_registry(raw: dict) -> dict:
    model = _dict(raw.get("model"))
    supplied_subject = _dict(raw.get("subject"))
    name = _text(supplied_subject.get("model_name"), model.get("name"), raw.get("model_name"), default="unknown-model")
    version = _text(supplied_subject.get("model_version"), model.get("version"), raw.get("model_version"), default="unknown-version")
    source_type = _event_type(raw.get("event_type") or raw.get("action") or raw.get("type"), "MODEL_REGISTRY_EVENT")
    aliases = {
        "PROMOTED": "MODEL_PROMOTED", "MODEL_PROMOTION": "MODEL_PROMOTED",
        "APPROVED": "APPROVAL_GRANTED", "EVALUATION_COMPLETED": "MODEL_EVALUATION_COMPLETED",
        "ARTIFACT_CREATED": "ARTIFACT_REGISTERED",
    }
    event_type = aliases.get(source_type, source_type)
    actor = _dict(raw.get("actor"))
    return {
        "source_id": raw.get("id") or raw.get("event_id"),
        "event_time": _time(raw),
        "event_type": event_type,
        "actor": {
            "id": _text(actor.get("id"), raw.get("user"), default="model-registry"),
            "type": actor.get("type", "SERVICE"), "roles": actor.get("roles", []),
            "ip": actor.get("ip"), "namespace": actor.get("namespace"),
        },
        "subject": _subject(raw, default_type="MODEL", default_id=f"{name}:{version}", model_name=name, model_version=version),
        "action": _text(raw.get("action"), event_type.lower()),
        "outcome": raw.get("outcome", "SUCCESS"),
        "environment": _environment(raw),
        "correlation_id": _text(raw.get("correlation_id"), raw.get("id"), default=f"model:{name}:{version}"),
        "trace_id": raw.get("trace_id"),
        "attributes": _dict(raw.get("attributes")),
        "labels": _labels(raw, "model-registry"),
    }


def llm_gateway(raw: dict) -> dict:
    model = _dict(raw.get("model"))
    actor = _dict(raw.get("actor"))
    safe_keys = {
        "pii_detected", "pii_handling_allowed", "model_approved", "prompt_injection_score",
        "data_exfiltration_detected", "prompt_fingerprint", "redacted_excerpt", "tenant_id",
        "token_count", "policy_version",
    }
    supplied = _dict(raw.get("attributes"))
    attributes = {key: value for key, value in {**raw, **supplied}.items() if key in safe_keys}
    name = _text(model.get("name"), raw.get("model_name"), default="unknown-model")
    version = _text(model.get("version"), raw.get("model_version"), default="unknown-version")
    actor_id = _text(actor.get("id"), raw.get("user_id"), raw.get("tenant_id"), default="unknown")
    return {
        "source_id": raw.get("request_id") or raw.get("id") or raw.get("event_id"),
        "event_time": _time(raw),
        "event_type": _event_type(raw.get("event_type"), "PROMPT_REQUEST"),
        "actor": {
            "id": actor_id, "type": actor.get("type", "USER"), "roles": actor.get("roles", []),
            "ip": actor.get("ip"), "namespace": actor.get("namespace"),
        },
        "subject": _subject(raw, default_type="MODEL_ENDPOINT", default_id=f"{name}:{version}", model_name=name, model_version=version),
        "action": _text(raw.get("action"), default="inference"),
        "outcome": raw.get("outcome", "SUCCESS"),
        "environment": _environment(raw),
        "correlation_id": _text(raw.get("correlation_id"), raw.get("request_id"), default=f"llm:{actor_id}"),
        "trace_id": raw.get("trace_id"),
        "attributes": attributes,
        "labels": _labels(raw, "llm-gateway"),
    }
