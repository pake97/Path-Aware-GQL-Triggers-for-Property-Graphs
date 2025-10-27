package org.lyon1;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.lyon1.trigger.TxContext;
import org.lyon1.trigger.CommittedContext;

import static com.google.common.collect.Iterables.size;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.lyon1.trigger.*;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.kernel.api.EntityCursor;
import com.google.common.collect.Iterators;
import org.neo4j.cypher.internal.physicalplanning.ast.NodeProperty;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.newapi.DefaultNodeCursor;
import org.neo4j.lang.CloseListener;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.lyon1.path.DeltaPathMatcher;

public class Listener implements TransactionEventListener<List<Listener.PlannedTrigger>> {

    private Log logger;

    public Listener(Log logger) {

        this.logger = logger;
    }


    private TriggerRegistry registry;
    private InMemoryOrchestrator orchestrator;
    private Log log;
    private DeltaPathMatcher pathMatcher;


    public Listener(Log log, InMemoryOrchestrator orchestrator, TriggerRegistry registry, DeltaPathMatcher pathMatcher) {
        this.log = log;
        this.orchestrator = orchestrator;
        this.registry = registry;
        this.pathMatcher = pathMatcher;
    }


    @Override
    public List<PlannedTrigger> beforeCommit(TransactionData data, Transaction tx, GraphDatabaseService db) throws Exception {
        final TriggerRegistry.Snapshot snap = registry.snapshot();

        //log.info("Snap : " + snap.toString());
        final TriggerRegistry.IndexView ix = snap.indexView();
        //log.info("IndexView : " + ix.toString());
        // 1) Collect label sets for created and (pre-)deleted nodes
        final Map<String, Set<String>> createdNodeLabels = new HashMap<>(16);
        for (Node n : data.createdNodes()) {
            //log.info("Created : " + n.getLabels().toString());
            createdNodeLabels.put(n.getElementId(), labelsOf(n));
        }

        // For deleted nodes we can't read labels off the Node; reconstruct from removedLabels() events
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
        final Set<String> nodeCandidateIds = new LinkedHashSet<>(32);
        // Node creates
        if (!createdNodeLabels.isEmpty()) {
            final Set<String> globalCreate = ix.globalNode().getOrDefault(TriggerRegistry.EventType.ON_CREATE, Set.of());
            nodeCandidateIds.addAll(globalCreate);
            for (Set<String> labels : createdNodeLabels.values()) {
                for (String lbl : labels) {
                    nodeCandidateIds.addAll(ix.nodeLabel().getOrDefault(lbl, Set.of()));
                }
            }
        }
        // Node deletes
        if (!deletedNodeLabels.isEmpty()) {
            final Set<String> globalDelete = ix.globalNode().getOrDefault(TriggerRegistry.EventType.ON_DELETE, Set.of());
            nodeCandidateIds.addAll(globalDelete);
            for (Set<String> labels : deletedNodeLabels.values()) {
                for (String lbl : labels) {
                    nodeCandidateIds.addAll(ix.nodeLabel().getOrDefault(lbl, Set.of()));
                }
            }
        }

        final Set<String> relCandidateIds = new LinkedHashSet<>(32);
        // Rel creates
        if (!createdRelTypes.isEmpty()) {
            relCandidateIds.addAll(ix.globalRel().getOrDefault(TriggerRegistry.EventType.ON_CREATE, Set.of()));
            for (String type : createdRelTypes.values()) {
                relCandidateIds.addAll(ix.relType().getOrDefault(type, Set.of()));
            }
        }
        // Rel deletes
        if (!deletedRelTypes.isEmpty()) {
            relCandidateIds.addAll(ix.globalRel().getOrDefault(TriggerRegistry.EventType.ON_DELETE, Set.of()));
            for (String type : deletedRelTypes.values()) {
                relCandidateIds.addAll(ix.relType().getOrDefault(type, Set.of()));
            }
        }

        // Label add/remove events can also trigger node-label based rules (optional; enable if desired)
        for (LabelEntry ev : data.assignedLabels()) {
            nodeCandidateIds.addAll(ix.nodeLabel().getOrDefault(ev.label().name(), Set.of()));
        }
        for (LabelEntry ev : data.removedLabels()) {
            nodeCandidateIds.addAll(ix.nodeLabel().getOrDefault(ev.label().name(), Set.of()));
        }

        // 4) Path candidates (via your matcher)
        final Set<String> pathSigs = pathMatcher.findCanonicalSignatures(data, db);

        final Set<String> pathCandidateIds = new LinkedHashSet<>(16);
        for (String sig : pathSigs) {
            pathCandidateIds.addAll(ix.pathSignature().getOrDefault(sig, Set.of()));
        }

        log.info("Node candidates : " + nodeCandidateIds.toString() + " size : " + nodeCandidateIds.size());
        log.info("Rel candidates : " + relCandidateIds.toString() + " size : " + relCandidateIds.size());
        log.info("path candidates : " + pathCandidateIds.toString() + " size : " + pathCandidateIds.size());

        // 5) De-dup IDs and load triggers (enabled only)
        final Map<String, TriggerRegistry.Trigger> byId = snap.byId();
        final List<TriggerRegistry.Trigger> candidates =
                Stream.of(nodeCandidateIds, relCandidateIds, pathCandidateIds)
                        .flatMap(Set::stream)
                        .distinct()
                        .map(byId::get)
                        .filter(Objects::nonNull)
                        .filter(TriggerRegistry.Trigger::enabled)
                        .collect(Collectors.toList());

        if (candidates.isEmpty()) {
            return List.of(); // state passed downstream (cheap)
        }

        log.info("Candidate triggers to evaluate : " + candidates.toString() + " size : " + candidates.size());

        // 6) Evaluate predicates & plan actions
        final List<PlannedTrigger> planned = new ArrayList<>(candidates.size());
        final TxContext ctx = new TxContext(snap.version(), data, tx, db);
        for (TriggerRegistry.Trigger t : candidates) {
            try {
                // You likely have richer predicate/action types; adapt here.
                if (predicate(t).apply(ctx)) {
                    //planned.add(new PlannedTrigger(t, ctx.dedupeKey(t)));

                    Consumer<CommittedContext> resolved = this.orchestrator.get(t.id())
                            .map(FullTrigger::action)
                            .orElse(c -> {
                            }); // no-op fallback

                    planned.add(new PlannedTrigger(t, ctx.dedupeKey(t), resolved));
                }
            } catch (Exception ex) {
                log.warn("Predicate for trigger " + t.id() + " threw, skipping. " + ex.getMessage(), ex);
            }
        }

        // 7) Order by priority then ID for determinism
        planned.sort(Comparator
                .comparingInt((PlannedTrigger p) -> p.trigger().priority())
                .thenComparing(p -> p.trigger().id()));
        log.info("Planned triggers  : " + planned.toString() + " size : " + planned.size());
        return planned;
    }

    @Override
    public void afterCommit(TransactionData data, List<PlannedTrigger> planned, GraphDatabaseService db) {
        if (planned == null || planned.isEmpty()) return;
        log.info("Planned triggers in afterCommit : " + planned.toString() + " size : " + planned.size());
        for (PlannedTrigger p : planned) {
            try {
                log.info("Executing action for id=" + p.trigger().id() +
                        " class=" + p.action().getClass().getName());

                p.action().accept(new CommittedContext(p.trigger(), data, db, p.dedupeKey()));
            } catch (Exception ex) {
                // Do not throw; commit is done. Consider sending to a DLQ.
                log.error("Trigger action failed id=" + p.trigger().id() + " key=" + p.dedupeKey() + ": " + ex.getMessage(), ex);
            }
        }
    }

    @Override
    public void afterRollback(TransactionData data, List<PlannedTrigger> planned, GraphDatabaseService db) {
        // Optionally log/metrics. No actions should run.
    }

    /* -------------------------------- Predicate/Action glue -------------------------------- */

    /**
     * Adapt your predicate storage to a Function here.
     * If your Trigger carries a compiled predicate, just read it.
     */
    private Function<TxContext, Boolean> predicate(TriggerRegistry.Trigger t) {
        // Placeholder: allow all; wire to your real predicate function repository.
        return ctx -> true;
    }

    /**
     * Adapt your action storage to a Consumer here.
     * If your Trigger carries a compiled action, just read it.
     */
    private java.util.function.Consumer<CommittedContext> action(TriggerRegistry.Trigger t) {
        // Placeholder: no-op; wire to your real action dispatch.
        return committed -> {
        };
    }

    /* -------------------------------- Helper types -------------------------------- */

    /**
     * What we'll pass to afterCommit.
     */
    public static final class PlannedTrigger {
        private final TriggerRegistry.Trigger trigger;
        private final String dedupeKey;
        private final Consumer<CommittedContext> action;

        public PlannedTrigger(TriggerRegistry.Trigger trigger, String dedupeKey, Consumer<CommittedContext> action) {
            this.trigger = trigger;
            this.dedupeKey = dedupeKey;
            this.action = action;
        }


        public TriggerRegistry.Trigger trigger() {
            return trigger;
        }

        public String dedupeKey() {
            return dedupeKey;
        }

        public java.util.function.Consumer<CommittedContext> action() {
            return action;
        }

        @Override
        public String toString() {
            return "PlannedTrigger{id=" + trigger.id() + ", priority=" + trigger.priority() + "}";
        }
    }





    /* -------------------------------- Private utilities -------------------------------- */

    private static Set<String> labelsOf(Node n) {
        Set<String> out = new LinkedHashSet<>();
        for (Label l : n.getLabels()) out.add(l.name());
        return out;
    }

    /**
     * Build a map: nodeId -> labels that node had before it was deleted.
     * Neo4j reports label removals via removedLabels() also for node deletions.
     */
    private static Map<String, Set<String>> preDeleteLabels(TransactionData data) {
        Map<String, Set<String>> map = new HashMap<>(16);
        Set<String> deletedIds = new HashSet<>();
        for (Node n : data.deletedNodes()) deletedIds.add(n.getElementId());
        for (LabelEntry ev : data.removedLabels()) {
            String id = ev.node().getElementId();
            if (!deletedIds.contains(id)) continue; // only deletions
            map.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(ev.label().name());
        }
        return map;
    }
}