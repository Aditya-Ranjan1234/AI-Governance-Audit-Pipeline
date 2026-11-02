-- Apply after 002. Two durable queues; immutable results; one revision per review chain.
BEGIN;
CREATE SCHEMA IF NOT EXISTS reviewer;
CREATE TABLE IF NOT EXISTS reviewer.jobs (
  job_id UUID PRIMARY KEY,
  source_run_id UUID NOT NULL REFERENCES investigator.runs(run_id),
  revision_run_id UUID,
  worker_version TEXT NOT NULL,
  model_id TEXT NOT NULL,
  revision_model_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','retry','succeeded','failed')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts>=0),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_token UUID, lease_until TIMESTAMPTZ, error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS reviewer_original_identity
  ON reviewer.jobs(source_run_id,worker_version,model_id,revision_model_id) WHERE revision_run_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS reviewer_revision_identity
  ON reviewer.jobs(revision_run_id,worker_version,model_id,revision_model_id) WHERE revision_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS reviewer_pending ON reviewer.jobs(status,available_at,created_at);
CREATE TABLE IF NOT EXISTS reviewer.runs (
  run_id UUID PRIMARY KEY, job_id UUID NOT NULL REFERENCES reviewer.jobs(job_id),
  attempt INTEGER NOT NULL, status TEXT NOT NULL CHECK (status IN ('succeeded','retry','failed')),
  payload JSONB, evidence JSONB NOT NULL DEFAULT '{}', error_code TEXT,
  finished_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(job_id,attempt)
);
CREATE TABLE IF NOT EXISTS reviewer.audit_events (
  audit_id UUID PRIMARY KEY, job_id UUID NOT NULL REFERENCES reviewer.jobs(job_id),
  attempt INTEGER NOT NULL, kind TEXT NOT NULL, details JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS investigator.revision_jobs (
  job_id UUID PRIMARY KEY,
  source_run_id UUID NOT NULL REFERENCES investigator.runs(run_id),
  review_run_id UUID NOT NULL UNIQUE REFERENCES reviewer.runs(run_id),
  worker_version TEXT NOT NULL, model_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','retry','succeeded','failed')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts>=0),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_token UUID, lease_until TIMESTAMPTZ, error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS investigator_revision_pending ON investigator.revision_jobs(status,available_at,created_at);
CREATE TABLE IF NOT EXISTS investigator.revision_runs (
  run_id UUID PRIMARY KEY, job_id UUID NOT NULL REFERENCES investigator.revision_jobs(job_id),
  attempt INTEGER NOT NULL, status TEXT NOT NULL CHECK (status IN ('succeeded','retry','failed')),
  payload JSONB, evidence JSONB NOT NULL DEFAULT '{}', error_code TEXT,
  finished_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(job_id,attempt)
);
CREATE TABLE IF NOT EXISTS investigator.revision_audit_events (
  audit_id UUID PRIMARY KEY, job_id UUID NOT NULL REFERENCES investigator.revision_jobs(job_id),
  attempt INTEGER NOT NULL, kind TEXT NOT NULL, details JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='reviewer_revision_run_fk' AND conrelid='reviewer.jobs'::regclass) THEN
    ALTER TABLE reviewer.jobs ADD CONSTRAINT reviewer_revision_run_fk
      FOREIGN KEY(revision_run_id) REFERENCES investigator.revision_runs(run_id);
  END IF;
END $$;

-- Invoker-rights triggers protect the communication contract, including direct queue inserts.
CREATE OR REPLACE FUNCTION reviewer.check_revision_request() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM reviewer.runs r JOIN reviewer.jobs j USING(job_id)
    WHERE r.run_id=NEW.review_run_id AND r.status='succeeded'
      AND r.payload->>'effective_verdict'='revision_required'
      AND j.revision_run_id IS NULL AND j.source_run_id=NEW.source_run_id
      AND j.revision_model_id=NEW.model_id
  ) THEN RAISE EXCEPTION 'Revision must reference an initial review requesting correction' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER revision_request_contract BEFORE INSERT ON investigator.revision_jobs
FOR EACH ROW EXECUTE FUNCTION reviewer.check_revision_request();

CREATE OR REPLACE FUNCTION reviewer.check_review_source() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM investigator.runs WHERE run_id=NEW.source_run_id AND status='succeeded') THEN
    RAISE EXCEPTION 'Review requires a successful immutable investigation' USING ERRCODE='23514';
  END IF;
  IF NEW.revision_run_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM investigator.revision_runs r JOIN investigator.revision_jobs q USING(job_id)
    JOIN reviewer.runs feedback ON feedback.run_id=q.review_run_id
    JOIN reviewer.jobs original ON original.job_id=feedback.job_id
    WHERE r.run_id=NEW.revision_run_id AND r.status='succeeded'
      AND q.source_run_id=NEW.source_run_id AND original.revision_run_id IS NULL
      AND original.worker_version=NEW.worker_version AND original.model_id=NEW.model_id
      AND original.revision_model_id=NEW.revision_model_id
  ) THEN RAISE EXCEPTION 'Revision review must stay in its original chain' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER review_source_contract BEFORE INSERT ON reviewer.jobs
FOR EACH ROW EXECUTE FUNCTION reviewer.check_review_source();

CREATE OR REPLACE VIEW reviewer.latest_results AS
SELECT DISTINCT ON (j.source_run_id,j.worker_version,j.model_id,j.revision_model_id)
  j.source_run_id, j.revision_run_id, j.worker_version, j.model_id, j.revision_model_id,
  original_job.finding_id, j.job_id, r.run_id AS review_run_id,
  r.payload->>'effective_verdict' AS verdict, r.payload AS review,
  COALESCE(revision.payload,original.report) AS report,
  original.evidence, r.finished_at
FROM reviewer.jobs j JOIN reviewer.runs r ON r.job_id=j.job_id AND r.status='succeeded'
JOIN investigator.runs original ON original.run_id=j.source_run_id
JOIN investigator.jobs original_job ON original_job.job_id=original.job_id
LEFT JOIN investigator.revision_runs revision ON revision.run_id=j.revision_run_id
ORDER BY j.source_run_id,j.worker_version,j.model_id,j.revision_model_id,
  (j.revision_run_id IS NOT NULL) DESC,r.finished_at DESC,r.run_id;

DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='governance_reviewer') THEN
    CREATE ROLE governance_reviewer NOLOGIN;
  END IF;
END $$;
GRANT USAGE ON SCHEMA investigator,reviewer TO governance_reviewer,governance_investigator;
GRANT SELECT ON investigator.jobs,investigator.runs,investigator.revision_jobs,investigator.revision_runs TO governance_reviewer;
GRANT SELECT,INSERT ON reviewer.jobs TO governance_reviewer;
GRANT UPDATE(status,attempts,available_at,lease_token,lease_until,error_code,updated_at) ON reviewer.jobs TO governance_reviewer;
GRANT SELECT,INSERT ON reviewer.runs,reviewer.audit_events TO governance_reviewer;
GRANT INSERT ON investigator.revision_jobs TO governance_reviewer;
GRANT SELECT ON investigator.revision_jobs TO governance_investigator;
GRANT UPDATE(status,attempts,available_at,lease_token,lease_until,error_code,updated_at) ON investigator.revision_jobs TO governance_investigator;
GRANT SELECT,INSERT ON investigator.revision_runs,investigator.revision_audit_events TO governance_investigator;
GRANT SELECT ON reviewer.jobs,reviewer.runs TO governance_investigator;
GRANT SELECT ON reviewer.latest_results TO governance_reviewer,governance_investigator;
COMMIT;
