CREATE SCHEMA IF NOT EXISTS aws_tableau;

CREATE TABLE IF NOT EXISTS aws_tableau.framework_status (
  day TIMESTAMPTZ,
  framework TEXT,
  finding_count BIGINT,
  high_or_critical_count BIGINT,
  average_risk NUMERIC,
  maximum_risk NUMERIC
);

CREATE TABLE IF NOT EXISTS aws_tableau.risk_trend (
  time_bucket TIMESTAMPTZ,
  producer TEXT,
  rule_id TEXT,
  baseline_risk NUMERIC,
  isolation_forest_risk NUMERIC,
  recommended_risk NUMERIC,
  finding_count BIGINT
);

CREATE TABLE IF NOT EXISTS aws_tableau.incident_history (
  finding_id UUID,
  detected_at TIMESTAMPTZ,
  rule_id TEXT,
  title TEXT,
  producer TEXT,
  severity_label TEXT,
  baseline_risk NUMERIC,
  isolation_forest_risk NUMERIC,
  risk_delta NUMERIC,
  recommended_risk NUMERIC,
  model_status TEXT,
  source_event_id UUID,
  evidence JSONB
);

CREATE TABLE IF NOT EXISTS aws_tableau.pipeline_latency (
  time_bucket TIMESTAMPTZ,
  producer TEXT,
  p50_ms DOUBLE PRECISION,
  p95_ms DOUBLE PRECISION,
  p99_ms DOUBLE PRECISION,
  samples BIGINT
);

CREATE TABLE IF NOT EXISTS aws_tableau.framework_status_next
  (LIKE aws_tableau.framework_status INCLUDING ALL);
CREATE TABLE IF NOT EXISTS aws_tableau.risk_trend_next
  (LIKE aws_tableau.risk_trend INCLUDING ALL);
CREATE TABLE IF NOT EXISTS aws_tableau.incident_history_next
  (LIKE aws_tableau.incident_history INCLUDING ALL);
CREATE TABLE IF NOT EXISTS aws_tableau.pipeline_latency_next
  (LIKE aws_tableau.pipeline_latency INCLUDING ALL);

CREATE OR REPLACE VIEW public.aws_tableau_framework_status AS
  SELECT * FROM aws_tableau.framework_status;
CREATE OR REPLACE VIEW public.aws_tableau_risk_trend AS
  SELECT * FROM aws_tableau.risk_trend;
CREATE OR REPLACE VIEW public.aws_tableau_incident_history AS
  SELECT * FROM aws_tableau.incident_history;
CREATE OR REPLACE VIEW public.aws_tableau_pipeline_latency AS
  SELECT * FROM aws_tableau.pipeline_latency;
