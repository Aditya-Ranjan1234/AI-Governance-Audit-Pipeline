from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import math
import os
from typing import Any

import numpy as np
from sklearn.ensemble import IsolationForest


RULES = [f"GOV-{number:03d}" for number in range(1, 17)]
PRODUCERS = ["KEYCLOAK", "KUBERNETES", "PIPELINE", "MODEL_REGISTRY", "LLM_GATEWAY"]


class OnlineIsolationScorer:
    """Bounded-history Isolation Forest with deterministic retraining."""

    def __init__(self, min_samples: int = 32, retrain_every: int = 25, history_size: int = 1000):
        self.min_samples = min_samples
        self.retrain_every = retrain_every
        self.history: deque[list[float]] = deque(maxlen=history_size)
        self.model: IsolationForest | None = None
        self.since_fit = 0

    def score(self, finding: dict[str, Any]) -> dict[str, Any]:
        finding = self.normalize_timestamps(finding)
        vector = self.features(finding)
        self.history.append(vector)
        self.since_fit += 1
        if len(self.history) >= self.min_samples and (self.model is None or self.since_fit >= self.retrain_every):
            self.model = IsolationForest(
                n_estimators=200,
                max_samples=min(256, len(self.history)),
                contamination="auto",
                random_state=42,
                n_jobs=1,
            ).fit(np.asarray(self.history, dtype=float))
            self.since_fit = 0

        baseline = float(finding["baseline_risk"])
        if self.model is None:
            ml_risk = None
            status = f"warming:{len(self.history)}/{self.min_samples}"
            recommended = baseline
            delta = None
        else:
            decision = float(self.model.decision_function(np.asarray([vector], dtype=float))[0])
            ml_risk = round(float(np.clip(50.0 - 100.0 * decision, 0.0, 100.0)), 2)
            status = "ready"
            recommended = round(max(baseline, ml_risk), 2)
            delta = round(ml_risk - baseline, 2)

        return {
            **finding,
            "isolation_forest_risk": ml_risk,
            "risk_delta": delta,
            "recommended_risk": recommended,
            "model_status": status,
        }

    @staticmethod
    def normalize_timestamps(finding: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(finding)
        for field in ("source_event_time", "detected_at", "window_start", "window_end"):
            value = normalized.get(field)
            if isinstance(value, (int, float)):
                normalized[field] = datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")
        return normalized

    @staticmethod
    def features(finding: dict[str, Any]) -> list[float]:
        rule_one_hot = [1.0 if finding.get("rule_id") == rule else 0.0 for rule in RULES]
        producer_one_hot = [1.0 if finding.get("producer") == producer else 0.0 for producer in PRODUCERS]
        at = datetime.fromisoformat(str(finding["source_event_time"]).replace("Z", "+00:00"))
        angle = 2.0 * math.pi * at.hour / 24.0
        numeric = [
            float(finding["severity"]) / 5.0,
            float(finding["frequency_band"]) / 5.0,
            float(finding["baseline_risk"]) / 100.0,
            math.sin(angle),
            math.cos(angle),
        ]
        return numeric + rule_one_hot + producer_one_hot


def scorer_from_environment() -> OnlineIsolationScorer:
    return OnlineIsolationScorer(
        min_samples=int(os.getenv("RISK_MODEL_MIN_SAMPLES", "32")),
        retrain_every=int(os.getenv("RISK_MODEL_RETRAIN_EVERY", "25")),
        history_size=int(os.getenv("RISK_MODEL_HISTORY_SIZE", "1000")),
    )
