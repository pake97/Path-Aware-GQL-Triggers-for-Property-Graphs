package org.lyon1;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.lyon1.trigger.TxContext;
import org.lyon1.trigger.CommittedContext;

import java.util.stream.Stream;

import org.lyon1.trigger.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.logging.Log;

public class Listener implements TransactionEventListener<List<Listener.PlannedTrigger>> {

    private Log logger;

    public Listener(Log logger) {

        this.logger = logger;
    }

    private TriggerRegistryInterface registry;
    private InMemoryOrchestrator orchestrator;
    private Log log;

    public Listener(Log log, InMemoryOrchestrator orchestrator, TriggerRegistryInterface registry) {
        this.log = log;
        this.orchestrator = orchestrator;
        this.registry = registry;
    }

    @Override
    public List<PlannedTrigger> beforeCommit(TransactionData data, Transaction tx, GraphDatabaseService db)
            throws Exception {
        final TriggerRegistryInterface.Snapshot snap = registry.snapshot();

        // log.info("Snap : " + snap.toString());
        final TriggerRegistryInterface.IndexView ix = snap.indexView();
        // log.info("IndexView : " + ix.toString());
        // 1) Collect label sets for created and (pre-)deleted nodes
        final Map<String, Set<String>> createdNodeLabels = new HashMap<>(16);

        for (Node n : data.createdNodes()) {
            // log.info("Created : " + n.getLabels().toString());
            createdNodeLabels.put(n.getElementId(), labelsOf(n));

        }

        // For deleted nodes we can't read labels off the Node; reconstruct from
        // removedLabels() events
        final Map<String, Set<String>> deletedNodeLabels = preDeleteLabels(data);

        // 2) Relationship types for created/deleted rels
        final Map<String, String> createdRelTypes = new HashMap<>(16);
        for (Relationship r : data.createdRelationships()) {
            createdRelTypes.put(r.getElementId(), r.getType().name());
        }
        final Map<String, String> deletedRelTypes = new HashMap<>(16);
        for (Relationship r : data.deletedRelationships()) {
            deletedRelTypes.put(r.getElementId(), r.getType().name());
        }

        // 3) Candidate IDs via indexes
        // final Set<String> nodeCandidateIds = new LinkedHashSet<>(32);
        final List<String> nodeCandidateIds = new ArrayList<>();


        // Node creates
        if (!createdNodeLabels.isEmpty()) {
            // final Set<String> globalCreate =
            // ix.globalNode().getOrDefault(TriggerRegistry.EventType.ON_CREATE, Set.of());

            // nodeCandidateIds.addAll(globalCreate);
            for (Set<String> labels : createdNodeLabels.values()) {
                // for (String lbl : labels) {
                nodeCandidateIds
                        .addAll(this.registry.candidatesForNode(TriggerRegistryInterface.EventType.ON_CREATE, labels));
                // prima era così :
                // nodeCandidateIds.addAll(ix.nodeLabel().getOrDefault(lbl, Set.of()));
                // }
            }
        }
        // Node deletes
        if (!deletedNodeLabels.isEmpty()) {
            // final Set<String> globalDelete =
            // ix.globalNode().getOrDefault(TriggerRegistry.EventType.ON_DELETE, Set.of());
            // nodeCandidateIds.addAll(globalDelete);
            for (Set<String> labels : deletedNodeLabels.values()) {

                nodeCandidateIds
                        .addAll(this.registry.candidatesForNode(TriggerRegistryInterface.EventType.ON_DELETE, labels));
                // nodeCandidateIds.addAll(ix.nodeLabel().getOrDefault(lbl, Set.of()));

            }
        }

        // final Set<String> relCandidateIds = new LinkedHashSet<>(32);
        final List<String> relCandidateIds = new ArrayList<>();
        // Rel creates
        if (!createdRelTypes.isEmpty()) {
            // relCandidateIds.addAll(ix.globalRel().getOrDefault(TriggerRegistry.EventType.ON_CREATE,
            // Set.of()));
            for (String type : createdRelTypes.values()) {
                relCandidateIds.addAll(
                        this.registry.candidatesForRelationship(TriggerRegistryInterface.EventType.ON_CREATE, type));
            }
        }
        // Rel deletes
        if (!deletedRelTypes.isEmpty()) {
            // relCandidateIds.addAll(ix.globalRel().getOrDefault(TriggerRegistry.EventType.ON_DELETE,
            // Set.of()));
            for (String type : deletedRelTypes.values()) {
                // relCandidateIds.addAll(ix.relType().getOrDefault(type, Set.of()));
                relCandidateIds.addAll(
                        this.registry.candidatesForRelationship(TriggerRegistryInterface.EventType.ON_DELETE, type));
            }
        }
        long t1 = System.nanoTime();
        // 4) Path candidates
        final Map<String, List<TriggerRegistryInterface.PathMatch>> pathMatches = new HashMap<>();

        // Check for node changes
        for (Node n : data.createdNodes()) {
            mergeMatches(pathMatches, ix.pathMonitor().findMatches(tx, "node:" + n.getElementId()));
        }
        for (Node n : data.deletedNodes()) {
            mergeMatches(pathMatches, ix.pathMonitor().findMatches(tx, "node:" + n.getElementId()));
        }

        // Check for relationship changes
        for (Relationship r : data.createdRelationships()) {
            mergeMatches(pathMatches, ix.pathMonitor().findMatches(tx, "rel:" + r.getElementId()));
        }
        for (Relationship r : data.deletedRelationships()) {
            mergeMatches(pathMatches, ix.pathMonitor().findMatches(tx, "rel:" + r.getElementId()));
        }

        log.info("Node candidates count : " + nodeCandidateIds.size());
        log.info("Rel candidates count : " + relCandidateIds.size());
        log.info("Path triggers matched : " + pathMatches.keySet());

        log.info("Node candidates : " + nodeCandidateIds.toString() + " size : " + nodeCandidateIds.size());
        log.info("Rel candidates : " + relCandidateIds.toString() + " size : " + relCandidateIds.size());
        log.info("path candidates : " + pathMatches.keySet() + " size : " + pathMatches.size());

        // 5) De-dup IDs and load triggers (enabled only)
        // rewrite this : i don't need to de duplicate because i'm using lists not sets

        final Map<String, TriggerRegistryInterface.Trigger> byId = snap.byId();
        final List<TriggerRegistryInterface.Trigger> candidates = Stream
                .of(nodeCandidateIds, relCandidateIds, new ArrayList<>(pathMatches.keySet()))
                .flatMap(Collection::stream)
                .distinct()
                .map(byId::get)
                .filter(Objects::nonNull)
                .filter(TriggerRegistryInterface.Trigger::enabled)
                .collect(Collectors.toList());

        // final Map<String, TriggerRegistry.Trigger> byId = snap.byId();
        // final List<TriggerRegistry.Trigger> candidates =
        // Stream.of(nodeCandidateIds, relCandidateIds, pathCandidateIds)
        // .flatMap(Set::stream)
        // .distinct()
        // .map(byId::get)
        // .filter(Objects::nonNull)
        // .filter(TriggerRegistry.Trigger::enabled)
        // .collect(Collectors.toList());

        if (candidates.isEmpty()) {
            return List.of(); // state passed downstream (cheap)
        }

        log.info("Candidate triggers to evaluate : " + candidates.toString() + " size : " + candidates.size());

        // 6) Evaluate predicates & plan actions
        final List<PlannedTrigger> planned = new ArrayList<>(candidates.size());
        final TxContext ctx = new TxContext(snap.version(), data, tx, db);
        for (TriggerRegistryInterface.Trigger t : candidates) {
            try {
                if (predicate(t).apply(ctx)) {
                    Consumer<CommittedContext> resolved = this.orchestrator.get(t.id())
                            .map(FullTrigger::action)
                            .orElse(c -> {
                            }); // no-op fallback

                    List<TriggerRegistryInterface.PathMatch> matches = pathMatches.getOrDefault(t.id(), List.of());
                    planned.add(new PlannedTrigger(t, ctx.dedupeKey(t), resolved, matches));
                }
            } catch (Exception ex) {
                log.warn("Predicate for trigger " + t.id() + " threw, skipping. " + ex.getMessage(), ex);
            }
        }
        long t2 = System.nanoTime();
        log.error("Activation time. Time ns : " + (t2 - t1));
        // 7) Order by priority then input order
        Comparator<PlannedTrigger> comparator = Comparator
                .comparingInt((PlannedTrigger p) -> p.trigger().priority())
                .thenComparing(p -> p.trigger().order());

        List<PlannedTrigger> beforeCommit = new ArrayList<>();
        List<PlannedTrigger> afterCommit = new ArrayList<>();

        for (PlannedTrigger p : planned) {
            if (p.trigger().time() == TriggerRegistryInterface.Time.BEFORE_COMMIT) { // adjust enum/type name if
                                                                                     // different
                beforeCommit.add(p);
            } else if (p.trigger().time() == TriggerRegistryInterface.Time.AFTER_COMMIT) {
                afterCommit.add(p);
            } else {
                // Optional: log or decide a default bucket
                log.warn("Trigger {} has unexpected time {}", p.trigger().id(), p.trigger().time());
            }
        }

        // Sort both lists deterministically
        beforeCommit.sort(comparator);
        afterCommit.sort(comparator);

        log.info("Planned triggers BEFORE COMMIT : " + beforeCommit.toString());

        // 8) Execute BEFORE_COMMIT actions now
        for (PlannedTrigger p : beforeCommit) {
            try {
                p.action().accept(new CommittedContext(p.trigger(), data, db, p.dedupeKey(), p.matches()));
            } catch (Exception ex) {
                log.warn("Action for BEFORE_COMMIT trigger " + p.trigger().id() + " threw. " + ex.getMessage(), ex);
            }
        }
        return afterCommit;
    }

    @Override
    public void afterCommit(TransactionData data, List<PlannedTrigger> planned, GraphDatabaseService db) {
        long t1 = System.nanoTime();
        if (planned == null || planned.isEmpty())
            return;
        log.info("Planned triggers in AFTER COMMIT : " + planned.toString());
        for (PlannedTrigger p : planned) {
            try {
                log.info("Executing action for id=" + p.trigger().id() +
                        " class=" + p.action().getClass().getName());

                p.action().accept(new CommittedContext(p.trigger(), data, db, p.dedupeKey(), p.matches()));
            } catch (Exception ex) {
                // Do not throw; commit is done. Consider sending to a DLQ.
                log.error("Trigger action failed id=" + p.trigger().id() + " key=" + p.dedupeKey() + ": "
                        + ex.getMessage(), ex);
            }
        }
        long t2 = System.nanoTime();
        log.error("Execution Time. Time ns : " + (t2 - t1));
    }

    @Override
    public void afterRollback(TransactionData data, List<PlannedTrigger> planned, GraphDatabaseService db) {
        // Optionally log/metrics. No actions should run.
    }

    /*
     * -------------------------------- Predicate/Action glue
     * --------------------------------
     */

    /**
     * Adapt your predicate storage to a Function here.
     * If your Trigger carries a compiled predicate, just read it.
     */
    private Function<TxContext, Boolean> predicate(TriggerRegistryInterface.Trigger t) {
        // Placeholder: allow all; wire to your real predicate function repository.
        return ctx -> true;
    }

    /**
     * Adapt your action storage to a Consumer here.
     * If your Trigger carries a compiled action, just read it.
     */
    private java.util.function.Consumer<CommittedContext> action(TriggerRegistryInterface.Trigger t) {
        // Placeholder: no-op; wire to your real action dispatch.
        return committed -> {
        };
    }

    /*
     * -------------------------------- Helper types
     * --------------------------------
     */

    /**
     * What we'll pass to afterCommit.
     */
    public static final class PlannedTrigger {
        private final TriggerRegistryInterface.Trigger trigger;
        private final String dedupeKey;
        private final Consumer<CommittedContext> action;
        private final List<TriggerRegistryInterface.PathMatch> matches;

        public PlannedTrigger(TriggerRegistryInterface.Trigger trigger, String dedupeKey,
                Consumer<CommittedContext> action, List<TriggerRegistryInterface.PathMatch> matches) {
            this.trigger = trigger;
            this.dedupeKey = dedupeKey;
            this.action = action;
            this.matches = matches;
        }

        public TriggerRegistryInterface.Trigger trigger() {
            return trigger;
        }

        public String dedupeKey() {
            return dedupeKey;
        }

        public List<TriggerRegistryInterface.PathMatch> matches() {
            return matches;
        }

        public java.util.function.Consumer<CommittedContext> action() {
            return action;
        }

        @Override
        public String toString() {
            return "PlannedTrigger{id=" + trigger.id() + ", priority=" + trigger.priority() + "}";
        }
    }

    /*
     * -------------------------------- Private utilities
     * --------------------------------
     */

    private static Set<String> labelsOf(Node n) {
        Set<String> out = new LinkedHashSet<>();
        for (Label l : n.getLabels())
            out.add(l.name());
        return out;
    }

    /**
     * Build a map: nodeId -> labels that node had before it was deleted.
     * Neo4j reports label removals via removedLabels() also for node deletions.
     */
    private static Map<String, Set<String>> preDeleteLabels(TransactionData data) {
        Map<String, Set<String>> map = new HashMap<>(16);
        Set<String> deletedIds = new HashSet<>();
        for (Node n : data.deletedNodes())
            deletedIds.add(n.getElementId());
        for (LabelEntry ev : data.removedLabels()) {
            String id = ev.node().getElementId();
            if (!deletedIds.contains(id))
                continue; // only deletions
            map.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(ev.label().name());
        }
        return map;
    }

    private static void mergeMatches(Map<String, List<TriggerRegistryInterface.PathMatch>> target,
            Map<String, List<TriggerRegistryInterface.PathMatch>> source) {
        for (Map.Entry<String, List<TriggerRegistryInterface.PathMatch>> entry : source.entrySet()) {
            target.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
        }
    }
}
