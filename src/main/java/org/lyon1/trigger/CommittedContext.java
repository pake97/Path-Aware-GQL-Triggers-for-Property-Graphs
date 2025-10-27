package org.lyon1.trigger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;

/**
 * Context object passed to actions in afterCommit.
 */
public final class CommittedContext {
    private final TriggerRegistry.Trigger trigger;
    private final TransactionData data;
    private final GraphDatabaseService db;
    private final String dedupeKey;

    public CommittedContext(TriggerRegistry.Trigger trigger, TransactionData data, GraphDatabaseService db, String dedupeKey) {
        this.trigger = trigger;
        this.data = data;
        this.db = db;
        this.dedupeKey = dedupeKey;
    }

    public TriggerRegistry.Trigger trigger() {
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