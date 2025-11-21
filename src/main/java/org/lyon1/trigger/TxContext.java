package org.lyon1.trigger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;

import java.util.Objects;

/**
 * Read-only context available during beforeCommit predicate evaluation.
 */
public final class TxContext {
    private final long registryVersion;
    private final TransactionData data;
    private final Transaction tx;
    private final GraphDatabaseService db;

    public TxContext(long registryVersion, TransactionData data, Transaction tx, GraphDatabaseService db) {
        this.registryVersion = registryVersion;
        this.data = data;
        this.tx = tx;
        this.db = db;
    }

    public long registryVersion() {
        return registryVersion;
    }

    public TransactionData data() {
        return data;
    }

    public Transaction tx() {
        return tx;
    }

    public GraphDatabaseService db() {
        return db;
    }

    /**
     * Build a stable dedupe key: tx id +
     * id (extend with element ids if needed).
     */
    public String dedupeKey(TriggerRegistryInterface.Trigger t) {
        // Neo4j doesn't expose tx id here; using hash of data snapshots is one option.
        // Replace with your tx id if you propagate it via tx metadata.
        int h = Objects.hash(System.identityHashCode(data), t.id(), registryVersion);
        return t.id() + ":" + h;
    }
}