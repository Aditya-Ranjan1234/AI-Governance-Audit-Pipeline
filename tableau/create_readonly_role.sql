-- Run as the database owner after replacing the password through your secret-management workflow.
CREATE ROLE tableau_reader LOGIN PASSWORD 'REPLACE_ME';
GRANT CONNECT ON DATABASE governance TO tableau_reader;
GRANT USAGE ON SCHEMA public TO tableau_reader;
GRANT SELECT ON tableau_framework_status, tableau_risk_trend,
  tableau_incident_history, tableau_pipeline_latency TO tableau_reader;

