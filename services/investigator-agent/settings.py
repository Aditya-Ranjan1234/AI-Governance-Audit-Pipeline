"""Validated process configuration. No credentials or default model are baked in."""

from dataclasses import dataclass
import os
from pathlib import Path

AGENT_VERSION = "investigator-v1.3"
DEFAULT_RULES = ("GOV-002", "GOV-003", "GOV-004", "GOV-006", "GOV-011", "GOV-012")


def integer(name: str, default: int, low: int, high: int) -> int:
    value = int(os.getenv(name, str(default)))
    if not low <= value <= high:
        raise ValueError(f"{name} must be between {low} and {high}")
    return value


@dataclass(frozen=True)
class Settings:
    dsn: str
    model_id: str
    region: str
    rules_path: Path
    rules: tuple[str, ...] = DEFAULT_RULES
    poll_seconds: int = 5
    max_steps: int = 6
    max_attempts: int = 3
    timeout_seconds: int = 120
    max_tokens: int = 1500

    @classmethod
    def from_env(cls, require_model: bool = True):
        dsn = os.getenv("INVESTIGATOR_POSTGRES_DSN", "")
        model = os.getenv("BEDROCK_MODEL_ID", "").strip()
        region = os.getenv("AWS_REGION", "").strip()
        if not dsn:
            raise ValueError("INVESTIGATOR_POSTGRES_DSN is required")
        if require_model and (not model or not region):
            raise ValueError("BEDROCK_MODEL_ID and AWS_REGION are required; select a Converse tool-use model")
        rules = tuple(
            x.strip() for x in os.getenv("INVESTIGATOR_RULE_IDS", ",".join(DEFAULT_RULES)).split(",") if x.strip()
        )
        if not rules or any(r not in {f"GOV-{n:03d}" for n in range(1, 17)} for r in rules):
            raise ValueError("INVESTIGATOR_RULE_IDS must contain known GOV-001..GOV-016 IDs")
        root = Path(__file__).resolve().parents[2]
        return cls(
            dsn,
            model,
            region,
            Path(os.getenv("INVESTIGATOR_RULES_PATH", str(root / "config/rules.yml"))),
            rules,
            integer("INVESTIGATOR_POLL_SECONDS", 5, 1, 300),
            integer("INVESTIGATOR_MAX_STEPS", 6, 2, 12),
            integer("INVESTIGATOR_MAX_ATTEMPTS", 3, 1, 5),
            integer("INVESTIGATOR_TIMEOUT_SECONDS", 120, 10, 300),
            integer("INVESTIGATOR_MAX_TOKENS", 1500, 256, 4096),
        )
