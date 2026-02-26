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
    public Map<String, List<TriggerRegistryInterface.PathMatch>> findMatches(org.neo4j.graphdb.Transaction tx,
            String elementIdWithPrefix) {
        if (elementIdWithPrefix == null || !elementIdWithPrefix.contains(":")) {
            return Map.of();
        }

        String[] parts = elementIdWithPrefix.split(":", 2);
        String type = parts[0];
        String elementId = parts[1];

        Map<String, List<TriggerRegistryInterface.PathMatch>> matchesFound = new HashMap<>();

        for (TriggerRegistryInterface.Trigger trigger : triggers) {
            if (trigger.scope() == TriggerRegistryInterface.Scope.PATH && trigger.enabled()
                    && trigger.activation() instanceof TriggerRegistryInterface.PathActivation pa) {

                List<TriggerRegistryInterface.PathMatch> triggerMatches = getMatchesForTrigger(tx, trigger, pa, type,
                        elementId);
                if (!triggerMatches.isEmpty()) {
                    matchesFound.put(trigger.id(), triggerMatches);
                }
            }
        }

        return matchesFound;
    }

    private List<TriggerRegistryInterface.PathMatch> getMatchesForTrigger(org.neo4j.graphdb.Transaction tx,
            TriggerRegistryInterface.Trigger trigger,
            TriggerRegistryInterface.PathActivation pa, String elementType,
            String elementId) {
        String pattern = pa.canonicalSignature();
        org.lyon1.path.GraphPath path = org.lyon1.automaton.PatternParser.parsePattern(pattern);
        List<org.lyon1.path.GraphElement> elements = path.getElements();

        List<TriggerRegistryInterface.PathMatch> results = new ArrayList<>();

        for (int i = 0; i < elements.size(); i++) {
            org.lyon1.path.GraphElement el = elements.get(i);
            boolean isNodeMatch = elementType.equals("node") && el.isNode();
            boolean isRelMatch = elementType.equals("rel") && el.isRelationship();

            if (isNodeMatch || isRelMatch) {
                String query = buildAnchoredQuery(elements, i);
                results.addAll(executeMatchQuery(tx, query, elementId));
            }
        }
        return results;
    }

    private String buildAnchoredQuery(List<org.lyon1.path.GraphElement> elements, int anchorIndex) {
        StringBuilder sb = new StringBuilder("MATCH p = ");
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
        sb.append(
                " WHERE elementId(anchor) = $id RETURN [node in nodes(p) | elementId(node)] as nodeIds, [rel in relationships(p) | elementId(rel)] as relIds");
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private List<TriggerRegistryInterface.PathMatch> executeMatchQuery(org.neo4j.graphdb.Transaction tx, String query,
            String elementId) {
        List<TriggerRegistryInterface.PathMatch> matches = new ArrayList<>();
        try (org.neo4j.graphdb.Result result = tx.execute(query, Map.of("id", elementId))) {
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                List<String> nodeIds = (List<String>) row.get("nodeIds");
                List<String> relIds = (List<String>) row.get("relIds");
                matches.add(new TriggerRegistryInterface.PathMatch(nodeIds, relIds));
            }
        } catch (Exception e) {
            if (log != null)
                log.error("Error executing path match query: " + query + " | " + e.getMessage());
        }
        return matches;
    }
}
