-- Apply as a database owner with CREATEROLE, after 001_schema.sql.
-- This migration is additive and safe to run more than once.
BEGIN;
CREATE SCHEMA IF NOT EXISTS investigator;
CREATE TABLE IF NOT EXISTS investigator.jobs (
  job_id UUID PRIMARY KEY,
  finding_id UUID NOT NULL REFERENCES public.compliance_findings(finding_id),
  agent_version TEXT NOT NULL,
  policy_hash TEXT NOT NULL,
  model_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','retry','succeeded','failed')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_until TIMESTAMPTZ,
  lease_token UUID,
  error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (finding_id, agent_version, policy_hash, model_id)
);
CREATE INDEX IF NOT EXISTS idx_investigator_pending ON investigator.jobs (status, available_at, created_at);
CREATE TABLE IF NOT EXISTS investigator.runs (
  run_id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES investigator.jobs(job_id),
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('succeeded','retry','failed')),
  report JSONB,
  evidence JSONB NOT NULL DEFAULT '{}',
  error_code TEXT,
  finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (job_id, attempt)
);
CREATE TABLE IF NOT EXISTS investigator.audit_events (
  audit_id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES investigator.jobs(job_id),
  attempt INTEGER NOT NULL,
  kind TEXT NOT NULL,
  details JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_investigator_audit_job ON investigator.audit_events (job_id, created_at);
CREATE OR REPLACE VIEW investigator.latest_reports AS
SELECT DISTINCT ON (j.finding_id)
  j.finding_id, j.job_id, j.agent_version, j.policy_hash, j.model_id,
  f.rule_id, f.title, f.severity_label, f.recommended_risk,
  r.run_id, r.report, r.evidence, r.finished_at
FROM investigator.jobs j
JOIN investigator.runs r ON r.job_id=j.job_id AND r.status='succeeded'
JOIN public.compliance_findings f ON f.finding_id=j.finding_id
ORDER BY j.finding_id, r.finished_at DESC, r.run_id;
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='governance_investigator') THEN
    CREATE ROLE governance_investigator NOLOGIN;
  END IF;
END $$;
GRANT USAGE ON SCHEMA public, investigator TO governance_investigator;
GRANT SELECT ON public.raw_events, public.compliance_findings TO governance_investigator;
GRANT SELECT, INSERT, UPDATE ON investigator.jobs TO governance_investigator;
GRANT SELECT, INSERT ON investigator.runs, investigator.audit_events TO governance_investigator;
GRANT SELECT ON investigator.latest_reports TO governance_investigator;
COMMIT;
