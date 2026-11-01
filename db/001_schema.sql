CREATE TABLE IF NOT EXISTS raw_events (
  event_id UUID PRIMARY KEY,
  schema_version TEXT NOT NULL,
  producer TEXT NOT NULL CHECK (producer IN ('KEYCLOAK','KUBERNETES','PIPELINE','MODEL_REGISTRY','LLM_GATEWAY')),
  event_type TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL,
  environment TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  payload JSONB NOT NULL,
  persisted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS compliance_findings (
  finding_id UUID PRIMARY KEY,
  rule_id TEXT NOT NULL CHECK (rule_id ~ '^GOV-[0-9]{3}$'),
  title TEXT NOT NULL,
  producer TEXT NOT NULL,
  source_event_id UUID NOT NULL,
  source_event_time TIMESTAMPTZ NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL,
  window_start TIMESTAMPTZ,
  window_end TIMESTAMPTZ,
  severity SMALLINT NOT NULL CHECK (severity BETWEEN 1 AND 5),
  severity_label TEXT NOT NULL CHECK (severity_label IN ('low','medium','high','critical')),
  frequency INTEGER NOT NULL CHECK (frequency > 0),
  frequency_band SMALLINT NOT NULL CHECK (frequency_band BETWEEN 1 AND 5),
  baseline_risk NUMERIC(5,2) NOT NULL CHECK (baseline_risk BETWEEN 0 AND 100),
  isolation_forest_risk NUMERIC(5,2) CHECK (isolation_forest_risk BETWEEN 0 AND 100),
  risk_delta NUMERIC(6,2),
  recommended_risk NUMERIC(5,2) NOT NULL CHECK (recommended_risk BETWEEN 0 AND 100),
  model_status TEXT,
  frameworks JSONB NOT NULL,
  evidence JSONB NOT NULL,
  persisted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS evaluation_ground_truth (
  run_id UUID NOT NULL,
  event_id UUID NOT NULL,
  expected_rule_id TEXT NOT NULL,
  injected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, event_id, expected_rule_id)
);

CREATE TABLE IF NOT EXISTS evaluation_events (
  run_id UUID NOT NULL,
  event_id UUID NOT NULL,
  injected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, event_id)
);

CREATE TABLE IF NOT EXISTS evaluation_runs (
  run_id UUID PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expected_findings INTEGER NOT NULL,
  injected_events INTEGER NOT NULL,
  completed_at TIMESTAMPTZ,
  results JSONB
);

CREATE INDEX IF NOT EXISTS idx_raw_event_time ON raw_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_producer ON raw_events (producer, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_findings_detected ON compliance_findings (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_findings_rule ON compliance_findings (rule_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_findings_frameworks ON compliance_findings USING GIN (frameworks);

CREATE OR REPLACE VIEW finding_framework_controls AS
SELECT
  f.finding_id,
  f.rule_id,
  f.title,
  f.detected_at,
  f.producer,
  f.severity,
  f.severity_label,
  f.recommended_risk,
  framework.key AS framework,
  control.value #>> '{}' AS control
FROM compliance_findings f
CROSS JOIN LATERAL jsonb_each(f.frameworks) AS framework(key, controls)
CROSS JOIN LATERAL jsonb_array_elements(framework.controls) AS control(value);

CREATE OR REPLACE VIEW tableau_framework_status AS
SELECT
  date_trunc('day', detected_at) AS day,
  framework,
  count(DISTINCT finding_id) AS finding_count,
  count(DISTINCT finding_id) FILTER (WHERE severity >= 4) AS high_or_critical_count,
  round(avg(recommended_risk), 2) AS average_risk,
  max(recommended_risk) AS maximum_risk
FROM finding_framework_controls
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tableau_risk_trend AS
SELECT
  date_bin('5 minutes', detected_at, TIMESTAMPTZ '2020-01-01') AS time_bucket,
  producer,
  rule_id,
  round(avg(baseline_risk), 2) AS baseline_risk,
  round(avg(isolation_forest_risk), 2) AS isolation_forest_risk,
  round(avg(recommended_risk), 2) AS recommended_risk,
  count(*) AS finding_count
FROM compliance_findings
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW tableau_incident_history AS
SELECT
  finding_id, detected_at, rule_id, title, producer, severity_label,
  baseline_risk, isolation_forest_risk, risk_delta, recommended_risk,
  model_status, source_event_id, evidence
FROM compliance_findings
ORDER BY detected_at DESC;

CREATE OR REPLACE VIEW tableau_pipeline_latency AS
SELECT
  date_bin('5 minutes', f.persisted_at, TIMESTAMPTZ '2020-01-01') AS time_bucket,
  f.producer,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (f.persisted_at - r.ingested_at)) * 1000) AS p50_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (f.persisted_at - r.ingested_at)) * 1000) AS p95_ms,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (f.persisted_at - r.ingested_at)) * 1000) AS p99_ms,
  count(*) AS samples
FROM compliance_findings f
JOIN raw_events r ON r.event_id = f.source_event_id
GROUP BY 1, 2;
