package org.lyon1.trigger;

import java.util.Map;
import java.util.Set;

import java.util.*;
import org.neo4j.graphdb.Transaction;
import java.util.function.Consumer;

/**
 * Thread-safe catalog of triggers with inverted indexes for fast candidate
 * lookup.
 * Typical usage:
 * - Writers/builders construct a new registry state (copy-on-write) and swap it
 * in one shot.
 * - Readers (Tx listeners) grab a read-only Snapshot once per tx and use
 * IndexView to find candidates.
 *
 * Contract:
 * - Implementations MUST be safe for concurrent reads and occasional writes.
 * - Snapshots and the maps they expose MUST be immutable (unmodifiable).
 * - version() MUST strictly increase on any structural change visible to
 * readers.
 */
public interface TriggerRegistryInterface {

    /* ======== Core model types expected to exist in your codebase ======== */
    enum EventType {
        ON_CREATE, ON_DELETE
    }

    enum Scope {
        NODE, RELATIONSHIP, PATH
    }

    enum Time {
        BEFORE_COMMIT, AFTER_COMMIT
    }

    sealed interface Activation permits NodeActivation, RelActivation, PathActivation {
    }

    record NodeActivation(Set<String> anyOfLabels,
            Map<String, Object> propertyEq,
            EventType eventType) implements Activation {
    }

    record RelActivation(String type,
            Set<String> startLabels,
            Set<String> endLabels,
            EventType eventType) implements Activation {
    }

    record PathActivation(String canonicalSignature,
            int maxLen,
            EventType eventType) implements Activation {
    }

    record Trigger(String id,
            Scope scope,
            Activation activation,
            int priority,
            int order,
            Time time,
            boolean enabled) {
    }

    /* ======================= Lifecycle / mutation ======================= */

    /**
     * @return monotonically increasing registry version; bumps on any visible
     *         change.
     */
    long version();

    /**
     * Registers a trigger and returns its (possibly generated) id.
     * If trigger.id() is null/blank, the implementation MUST generate a unique id.
     * MUST bump version on success.
     */
    String register(Trigger trigger);

    /**
     * Replace an existing trigger by id. No-op and returns false if not present.
     * MUST bump version on success.
     */
    boolean replace(Trigger trigger);

    /**
     * Enable/disable a trigger by id. Returns false if not found or no change.
     * MUST bump version on change.
     */
    boolean enable(String triggerId, boolean enabled);

    /**
     * Unregister by id. MUST bump version on success.
     */
    boolean unregister(String triggerId);

    /**
     * Atomically replace entire registry contents with the provided triggers
     * (copy-on-write swap).
     * MUST bump version if the new set differs from current.
     */
    void replaceAll(Collection<Trigger> triggers);

    /* =========================== Introspection ========================== */

    /**
     * Immutable point-in-time view. Obtain this once per transaction and reuse it.
     */
    Snapshot snapshot();

    /**
     * Lightweight query helpers (read ops do NOT mutate version).
     */
    Optional<Trigger> get(String triggerId);

    boolean contains(String triggerId);

    List<Trigger> list(); // unmodifiable copy, consistent with current version

    /**
     * Subscribe to version changes (e.g., for metrics, hot caches).
     * Implementations may deliver callbacks asynchronously. The callback MUST be
     * invoked
     * AFTER the new state becomes visible to readers.
     */
    void addListener(Consumer<Snapshot> onVersionChange);

    void removeListener(Consumer<Snapshot> onVersionChange);

    /* ======================== Candidate convenience ===================== */

    /**
     * Convenience: compute candidate trigger IDs for a node event given its labels.
     * Implementations may override for performance; default uses IndexView maps.
     */
    Set<String> candidatesForNode(EventType eventType, Iterable<String> labels);

    /**
     * Convenience: candidates for a relationship event given the relationship type.
     */
    Set<String> candidatesForRelationship(EventType eventType, String relType);

    /**
     * Convenience: candidates for a path event given a canonical signature.
     * (e.g., "(:A&:B)-[:T]->(:C)<-[:U]-(:D)")
     */
    Set<String> candidatesForPath(String canonicalSignature);

    /* ============================ Read-only API ========================= */

    /**
     * A frozen, immutable view of the registry at a specific version.
     * All collections returned by Snapshot (directly or via IndexView) MUST be
     * unmodifiable.
     */
    interface Snapshot {
        long version();

        /** All triggers by id (unmodifiable). */
        Map<String, Trigger> byId();

        /** Fast lookup structures used by the detector. */
        IndexView indexView();

        /** Convenience listings (stable iteration order recommended). */
        default List<Trigger> triggers() {
            return List.copyOf(byId().values());
        }

        default Optional<Trigger> get(String triggerId) {
            return Optional.ofNullable(byId().get(triggerId));
        }
    }

    interface PathMonitor {

        Set<String> findMatchingTriggers(Transaction tx, String canonicalSignature);

    }

    /**
     * Read-only inverted indexes (feature → trigger IDs).
     * Each Set<String> is the set of trigger IDs matching that feature.
     */
    interface IndexView {
        /**
         * Node label → trigger IDs (for NodeActivation with label predicates).
         * Keys are label names EXACT as stored in Neo4j (case-sensitive).
         */
        Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> nodeIndex();

        /**
         * Relationship type → trigger IDs (for RelActivation).
         */
        Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> relIndex();

        /**
         * Path type → trigger IDs (for PathActivation).
         */
        PathMonitor pathMonitor();

    }
}
