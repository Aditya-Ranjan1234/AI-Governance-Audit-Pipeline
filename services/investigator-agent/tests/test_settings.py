import pytest

from settings import Settings


def test_live_mode_requires_explicit_model_and_region(monkeypatch):
    monkeypatch.setenv("INVESTIGATOR_POSTGRES_DSN", "postgresql://test")
    monkeypatch.delenv("BEDROCK_MODEL_ID", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    with pytest.raises(ValueError, match="BEDROCK_MODEL_ID"):
        Settings.from_env()


def test_show_and_migrate_do_not_require_model_credentials(monkeypatch):
    monkeypatch.setenv("INVESTIGATOR_POSTGRES_DSN", "postgresql://test")
    monkeypatch.delenv("BEDROCK_MODEL_ID", raising=False)
    assert Settings.from_env(require_model=False).model_id == ""


@pytest.mark.parametrize(
    "variable,value",
    [("INVESTIGATOR_MAX_STEPS", "1000"), ("INVESTIGATOR_MAX_ATTEMPTS", "0"), ("INVESTIGATOR_RULE_IDS", "GOV-999")],
)
def test_configuration_bounds(monkeypatch, variable, value):
    monkeypatch.setenv("INVESTIGATOR_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setenv(variable, value)
    with pytest.raises(ValueError, match=variable):
        Settings.from_env(require_model=False)
