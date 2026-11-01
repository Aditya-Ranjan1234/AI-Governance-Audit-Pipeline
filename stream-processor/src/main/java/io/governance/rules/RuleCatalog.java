package io.governance.rules;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.governance.model.RuleDefinition;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RuleCatalog {
    private final Map<String, RuleDefinition> rules;

    private RuleCatalog(Map<String, RuleDefinition> rules) { this.rules = Map.copyOf(rules); }

    public static RuleCatalog load() {
        try (InputStream input = RuleCatalog.class.getResourceAsStream("/rules.yml")) {
            if (input == null) throw new IllegalStateException("rules.yml is missing from the classpath");
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, List<RuleDefinition>> root = mapper.readValue(input, new TypeReference<>() {});
            Map<String, RuleDefinition> byId = new LinkedHashMap<>();
            for (RuleDefinition definition : root.getOrDefault("rules", List.of())) byId.put(definition.id(), definition);
            if (byId.size() != 16) throw new IllegalStateException("Expected 16 rules, found " + byId.size());
            return new RuleCatalog(byId);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to load rule catalog", e);
        }
    }

    public RuleDefinition get(String id) {
        RuleDefinition definition = rules.get(id);
        if (definition == null) throw new IllegalArgumentException("Unknown rule " + id);
        return definition;
    }

    public int size() { return rules.size(); }
    public Map<String, RuleDefinition> all() { return rules; }
}

