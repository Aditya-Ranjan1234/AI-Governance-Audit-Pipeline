package io.governance.rules;

import io.governance.model.ComplianceFinding;
import io.governance.model.GovernanceEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class StatelessRuleEngine {
    private final RuleCatalog catalog;

    public StatelessRuleEngine(RuleCatalog catalog) { this.catalog = catalog; }

    public List<ComplianceFinding> evaluate(GovernanceEvent e) {
        List<ComplianceFinding> findings = new ArrayList<>();
        switch (e.producer()) {
            case KEYCLOAK -> evaluateKeycloak(e, findings);
            case KUBERNETES -> evaluateKubernetes(e, findings);
            case PIPELINE -> evaluatePipeline(e, findings);
            case MODEL_REGISTRY -> evaluateRegistry(e, findings);
            case LLM_GATEWAY -> evaluateGateway(e, findings);
        }
        return findings;
    }

    private void evaluateKeycloak(GovernanceEvent e, List<ComplianceFinding> out) {
        if (is(e, "ROLE_GRANT") && bool(e, "privileged_role") && !bool(e, "change_approved")) add(out, "GOV-002", e);
        if (is(e, "LOGIN_SUCCESS") && bool(e, "privileged_identity") && !bool(e, "mfa_used")) add(out, "GOV-003", e);
    }

    private void evaluateKubernetes(GovernanceEvent e, List<ComplianceFinding> out) {
        if (is(e, "CLUSTER_ROLE_BINDING") && "cluster-admin".equals(text(e, "cluster_role")) && !bool(e, "change_approved")) add(out, "GOV-004", e);
        if (is(e, "SECRET_ACCESS") && bool(e, "cross_namespace")) add(out, "GOV-005", e);
        if (is(e, "POD_CREATE") && (bool(e, "privileged") || !boolDefault(e, "image_approved", true))) add(out, "GOV-006", e);
    }

    private void evaluatePipeline(GovernanceEvent e, List<ComplianceFinding> out) {
        if (is(e, "DATASET_READ") && blank(text(e, "dataset_owner"))) add(out, "GOV-007", e);
        if (is(e, "SCHEMA_CHANGE") && !bool(e, "version_bumped")) add(out, "GOV-008", e);
        if (is(e, "DRIFT_EVALUATION") && number(e, "metric_value") > number(e, "threshold") && !"STARTED".equals(text(e, "remediation_status"))) add(out, "GOV-009", e);
        if (is(e, "BIAS_EVALUATION") && number(e, "metric_value") > number(e, "threshold") && !bool(e, "approved_exception")) add(out, "GOV-010", e);
    }

    private void evaluateRegistry(GovernanceEvent e, List<ComplianceFinding> out) {
        if (is(e, "ARTIFACT_REGISTERED") && (!bool(e, "signature_verified") || blank(text(e, "sha256")))) add(out, "GOV-013", e);
    }

    private void evaluateGateway(GovernanceEvent e, List<ComplianceFinding> out) {
        if (is(e, "PROMPT_REQUEST") && bool(e, "pii_detected") && !bool(e, "pii_handling_allowed")) add(out, "GOV-014", e);
        if (is(e, "PROMPT_REQUEST") && !boolDefault(e, "model_approved", true)) add(out, "GOV-015", e);
        if (is(e, "PROMPT_REQUEST") && (number(e, "prompt_injection_score") >= 0.8 || bool(e, "data_exfiltration_detected"))) add(out, "GOV-016", e);
    }

    private void add(List<ComplianceFinding> out, String ruleId, GovernanceEvent event) {
        out.add(FindingFactory.create(catalog.get(ruleId), event, 1, null, null, Map.of()));
    }

    private static boolean is(GovernanceEvent e, String type) { return type.equals(e.eventType()); }
    private static Object value(GovernanceEvent e, String key) { return e.attributes().get(key); }
    private static String text(GovernanceEvent e, String key) { Object v = value(e, key); return v == null ? null : String.valueOf(v); }
    private static boolean blank(String v) { return v == null || v.isBlank(); }
    private static boolean bool(GovernanceEvent e, String key) { return boolDefault(e, key, false); }
    private static boolean boolDefault(GovernanceEvent e, String key, boolean fallback) {
        Object v = value(e, key);
        if (v == null) return fallback;
        return v instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(v));
    }
    private static double number(GovernanceEvent e, String key) {
        Object v = value(e, key);
        if (v instanceof Number n) return n.doubleValue();
        try { return v == null ? 0 : Double.parseDouble(String.valueOf(v)); }
        catch (NumberFormatException ignored) { return 0; }
    }
}

