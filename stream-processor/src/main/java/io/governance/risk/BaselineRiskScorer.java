package io.governance.risk;

public final class BaselineRiskScorer {
    private BaselineRiskScorer() {}

    public static int frequencyBand(long frequency) {
        if (frequency <= 1) return 1;
        if (frequency <= 3) return 2;
        if (frequency <= 7) return 3;
        if (frequency <= 15) return 4;
        return 5;
    }

    public static double score(int severity, long frequency) {
        int boundedSeverity = Math.max(1, Math.min(5, severity));
        return boundedSeverity * frequencyBand(frequency) * 4.0;
    }

    public static String label(int severity) {
        return switch (severity) {
            case 1 -> "low";
            case 2, 3 -> "medium";
            case 4 -> "high";
            default -> "critical";
        };
    }
}

