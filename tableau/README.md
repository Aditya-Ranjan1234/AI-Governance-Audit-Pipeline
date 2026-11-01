# Tableau dashboards

For Tableau Public, use the local PostgreSQL reporting copy. Start or refresh it with:

```bash
./tableau/refresh_from_aws.sh
```

Connect Tableau to `127.0.0.1`, port `5432`, database `governance`, with the local Docker credentials from `docker-compose.yml`. Add these four local views as separate data sources:

- `aws_tableau_framework_status`
- `aws_tableau_risk_trend`
- `aws_tableau_incident_history`
- `aws_tableau_pipeline_latency`

The refresh opens a temporary authenticated tunnel to private RDS, copies the reporting data, and closes the tunnel. The AWS pipeline remains private. Incident evidence is replaced with a sanitized marker before it reaches the Tableau copy. Run the command again whenever you want a newer snapshot. Tableau Public publishes workbook data publicly, so use demonstration records only.

Tableau Desktop or Tableau Server can instead connect directly to RDS over the private network. Use a read-only database role and add the original four views as logical tables.

## Framework Status

Source: `tableau_framework_status`.

- Columns: `framework`.
- Rows: `finding_count` and `high_or_critical_count`.
- Color: `average_risk` with a fixed 0–100 scale.
- Filter: `day`, defaulting to the last 30 days.
- Tooltip: maximum risk and distinct findings.

This sheet shows finding volume and high-severity concentration for NIST AI RMF, ISO/IEC 42001, EU AI Act, NIST SP 800-53, and GDPR.

## Risk Trend

Source: `tableau_risk_trend`.

- Columns: continuous `time_bucket`.
- Rows: `baseline_risk`, `isolation_forest_risk`, and `recommended_risk` as Measure Values.
- Color: Measure Names.
- Filters: producer and rule ID.
- Add `finding_count` to the tooltip.

Keep the baseline and Isolation Forest lines visible together; their separation is the primary comparison required by the study.

## Incident History

Source: `tableau_incident_history`.

- Table columns: detected time, severity, rule ID, title, producer, baseline, Isolation Forest, recommended risk, and model status.
- Sort by detected time descending, then recommended risk descending.
- Add filters for severity, producer, rule ID, and date.
- Use `evidence` in the tooltip or a detail panel.

## Pipeline Health

Source: `tableau_pipeline_latency`.

- Plot p50, p95, and p99 by `time_bucket`.
- Filter by producer and add sample count to the tooltip.
- Add reference lines for the declared latency objective.

Create one dashboard with Framework Status and Risk Trend at the top and Incident History below. Create a second dashboard for Pipeline Health and evaluation results. Use a live PostgreSQL connection for the demonstration, or a 15-minute extract refresh when operational load makes live queries undesirable.
