package org.lyon1.trigger;

import java.util.*;

public final class AutomatonPathMonitor implements TriggerRegistryInterface.PathMonitor {

    private final Collection<TriggerRegistryInterface.Trigger> triggers;
    private final org.neo4j.logging.Log log;

    private AutomatonPathMonitor(Collection<TriggerRegistryInterface.Trigger> triggers, org.neo4j.logging.Log log) {
        this.triggers = triggers;
        this.log = log;
    }

    public static AutomatonPathMonitor fromTriggers(Collection<TriggerRegistryInterface.Trigger> triggers,
            org.neo4j.logging.Log log) {
        return new AutomatonPathMonitor(triggers, log);
    }

    @Override
    public Set<String> findMatchingTriggers(org.neo4j.graphdb.Transaction tx, String elementIdWithPrefix) {
        if (log != null)
            log.info("findMatchingTriggers called for " + elementIdWithPrefix + ". Registry size: " + triggers.size());

        if (elementIdWithPrefix == null || !elementIdWithPrefix.contains(":")) {
            return Set.of();
        }

        String[] parts = elementIdWithPrefix.split(":", 2);
        String type = parts[0];
        String elementId = parts[1];

        Set<String> matchingTriggerIds = new HashSet<>();

        for (TriggerRegistryInterface.Trigger trigger : triggers) {
            if (log != null)
                log.info("Considering trigger: " + trigger.id() + " scope=" + trigger.scope() + " enabled="
                        + trigger.enabled());

            if (trigger.scope() == TriggerRegistryInterface.Scope.PATH && trigger.enabled()
                    && trigger.activation() instanceof TriggerRegistryInterface.PathActivation pa) {
                if (matchesPath(tx, trigger, pa, type, elementId)) {
                    matchingTriggerIds.add(trigger.id());
                }
            }
        }

        return matchingTriggerIds;
    }

    private boolean matchesPath(org.neo4j.graphdb.Transaction tx, TriggerRegistryInterface.Trigger trigger,
            TriggerRegistryInterface.PathActivation pa, String elementType,
            String elementId) {
        String pattern = pa.canonicalSignature();
        org.lyon1.path.GraphPath path = org.lyon1.automaton.PatternParser.parsePattern(pattern);
        List<org.lyon1.path.GraphElement> elements = path.getElements();

        if (log != null)
            log.info("Parsed elements count: " + elements.size() + " for pattern: " + pattern);

        for (int i = 0; i < elements.size(); i++) {
            org.lyon1.path.GraphElement el = elements.get(i);
            boolean isNodeMatch = elementType.equals("node") && el.isNode();
            boolean isRelMatch = elementType.equals("rel") && el.isRelationship();

            if (isNodeMatch || isRelMatch) {
                String query = buildAnchoredQuery(elements, i);
                if (log != null)
                    log.info("Executing anchored query: " + query + " with id=" + elementId);

                if (executeCheck(tx, query, elementId)) {
                    if (log != null)
                        log.info("MATCH SUCCESS!");
                    return true;
                }
            }
        }
        return false;
    }

    private String buildAnchoredQuery(List<org.lyon1.path.GraphElement> elements, int anchorIndex) {
        StringBuilder sb = new StringBuilder("MATCH ");
        for (int i = 0; i < elements.size(); i++) {
            org.lyon1.path.GraphElement el = elements.get(i);
            String var = (i == anchorIndex) ? "anchor" : "";
            if (el.isNode()) {
                sb.append("(").append(var).append(":").append(el.getLabel()).append(")");
            } else {
                if (el.isIncoming()) {
                    sb.append("<-[").append(var).append(":").append(el.getLabel()).append("]-");
                } else {
                    sb.append("-[").append(var).append(":").append(el.getLabel()).append("]->");
                }
            }
        }
        sb.append(" WHERE elementId(anchor) = $id RETURN count(*) > 0 AS matched");
        return sb.toString();
    }

    private boolean executeCheck(org.neo4j.graphdb.Transaction tx, String query, String elementId) {
        try (org.neo4j.graphdb.Result result = tx.execute(query, Map.of("id", elementId))) {
            if (result.hasNext()) {
                Map<String, Object> row = result.next();
                Object val = row.get("matched");
                return Boolean.TRUE.equals(val);
            }
            return false;
        } catch (Exception e) {
            if (log != null)
                log.error("Error executing path check query: " + query + " | " + e.getMessage());
            return false;
        }
    }
}
