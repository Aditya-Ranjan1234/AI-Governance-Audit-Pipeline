package io.governance.model;

import java.util.List;
import java.util.Map;

public record RuleDefinition(
        String id,
        String title,
        int severity,
        String description,
        Map<String, List<String>> frameworks
) {}

