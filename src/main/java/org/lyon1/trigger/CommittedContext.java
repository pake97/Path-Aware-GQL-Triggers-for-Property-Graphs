package org.lyon1.trigger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;

/**
 * Context object passed to actions in afterCommit.
 */
public final class CommittedContext {
    private final TriggerRegistryInterface.Trigger trigger;
    private final TransactionData data;
    private final GraphDatabaseService db;
    private final String dedupeKey;
    private final java.util.List<TriggerRegistryInterface.PathMatch> matches;

    public CommittedContext(TriggerRegistryInterface.Trigger trigger, org.neo4j.graphdb.event.TransactionData data,
            org.neo4j.graphdb.GraphDatabaseService db, String dedupeKey,
            java.util.List<TriggerRegistryInterface.PathMatch> matches) {
        this.trigger = trigger;
        this.data = data;
        this.db = db;
        this.dedupeKey = dedupeKey;
        this.matches = matches != null ? java.util.List.copyOf(matches) : java.util.List.of();
    }

    public java.util.List<TriggerRegistryInterface.PathMatch> matches() {
        return matches;
    }

    public TriggerRegistryInterface.Trigger trigger() {
        return trigger;
    }

    public TransactionData data() {
        return data;
    }

    public GraphDatabaseService db() {
        return db;
    }

    public String dedupeKey() {
        return dedupeKey;
    }
}