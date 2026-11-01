from scorer import OnlineIsolationScorer


def finding(number=1, severity=2, frequency_band=1, baseline=8.0, producer="PIPELINE"):
    return {
        "finding_id": f"f-{number}",
        "rule_id": "GOV-007",
        "producer": producer,
        "source_event_time": "2026-01-01T09:00:00Z",
        "severity": severity,
        "frequency_band": frequency_band,
        "baseline_risk": baseline,
    }


def test_warms_before_minimum_samples():
    result = OnlineIsolationScorer(min_samples=3).score(finding())
    assert result["isolation_forest_risk"] is None
    assert result["recommended_risk"] == 8.0
    assert result["model_status"] == "warming:1/3"


def test_produces_bounded_comparison_after_training():
    scorer = OnlineIsolationScorer(min_samples=8, retrain_every=2)
    for i in range(8):
        scorer.score(finding(i))
    result = scorer.score(finding(99, severity=5, frequency_band=5, baseline=100, producer="LLM_GATEWAY"))
    assert result["model_status"] == "ready"
    assert 0 <= result["isolation_forest_risk"] <= 100
    assert result["recommended_risk"] == 100
    assert result["risk_delta"] == result["isolation_forest_risk"] - 100


def test_normalizes_legacy_epoch_timestamp():
    value = finding()
    value["source_event_time"] = 1767225600.0
    result = OnlineIsolationScorer(min_samples=3).score(value)
    assert result["source_event_time"] == "2026-01-01T00:00:00Z"
