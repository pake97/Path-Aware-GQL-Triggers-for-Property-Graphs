package org.lyon1.trigger;

import org.neo4j.dbms.api.DatabaseManagementService;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.logging.Log;

public abstract class TriggerRegistry implements TriggerRegistryInterface {

    protected final DatabaseManagementService dbms;
    protected final Log neoLog;
    /* ---------- Internal state ---------- */

    private static final class State {
        final long version;
        final Map<String, Trigger> byId; // unmodifiable
        final IndexViewImpl indexView; // unmodifiable sets inside
        final SnapshotImpl snapshot;

        State(long version,
                Map<String, Trigger> byId,
                IndexViewImpl indexView) {
            this.version = version;
            this.byId = byId;
            this.indexView = indexView;
            this.snapshot = new SnapshotImpl(version, byId, indexView);
        }
    }

    private final AtomicReference<State> ref;
    private final CopyOnWriteArrayList<Consumer<Snapshot>> listeners = new CopyOnWriteArrayList<>();

    public TriggerRegistry(DatabaseManagementService dbms, Log log) {
        this.dbms = dbms;
        this.neoLog = log;
        this.ref = new AtomicReference<>(new State(
                0L,
                Collections.unmodifiableMap(new LinkedHashMap<>()),
                IndexViewImpl.empty()));
    }

    /* ---------- TriggerRegistry ---------- */

    @Override
    public long version() {
        return ref.get().version;
    }

    @Override
    public String register(Trigger trigger) {
        Objects.requireNonNull(trigger, "trigger");
        String id = (trigger.id() != null && !trigger.id().isBlank())
                ? trigger.id()
                : UUID.randomUUID().toString();

        Trigger normalized = new Trigger(
                id,
                trigger.scope(),
                trigger.activation(),
                trigger.priority(),
                trigger.order(),
                trigger.time(),
                trigger.enabled());

        State before = ref.get();
        if (before.byId.containsKey(id)) {
            // Upsert semantics? The interface says "register" should add; if exists, we
            // replace.
            // Change to error if you prefer strict "must be new".
        }

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.put(id, normalized);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values()));

        ref.set(after);
        notifyListeners(after.snapshot);
        return id;
    }

    @Override
    public boolean replace(Trigger trigger) {
        Objects.requireNonNull(trigger, "trigger");
        String id = trigger.id();
        if (id == null || id.isBlank())
            return false;

        State before = ref.get();
        if (!before.byId.containsKey(id))
            return false;

        Trigger normalized = new Trigger(
                id,
                trigger.scope(),
                trigger.activation(),
                trigger.priority(),
                trigger.order(),
                trigger.time(),
                trigger.enabled());

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.put(id, normalized);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values()));

        ref.set(after);
        notifyListeners(after.snapshot);
        return true;
    }

    @Override
    public boolean enable(String triggerId, boolean enabled) {
        Objects.requireNonNull(triggerId, "triggerId");
        State before = ref.get();
        Trigger cur = before.byId.get(triggerId);
        if (cur == null || cur.enabled() == enabled)
            return false;

        Trigger updated = new Trigger(
                cur.id(), cur.scope(), cur.activation(), cur.priority(), cur.order(), cur.time(), enabled);

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.put(triggerId, updated);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values()));

        ref.set(after);
        notifyListeners(after.snapshot);
        return true;
    }

    @Override
    public boolean unregister(String triggerId) {
        Objects.requireNonNull(triggerId, "triggerId");
        State before = ref.get();
        if (!before.byId.containsKey(triggerId))
            return false;

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.remove(triggerId);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values()));

        ref.set(after);
        notifyListeners(after.snapshot);
        return true;
    }

    @Override
    public void replaceAll(Collection<Trigger> triggers) {
        Objects.requireNonNull(triggers, "triggers");
        Map<String, Trigger> nextById = new LinkedHashMap<>();
        for (Trigger t : triggers) {
            if (t == null)
                continue;
            String id = (t.id() != null && !t.id().isBlank()) ? t.id() : UUID.randomUUID().toString();
            nextById.put(id,
                    new Trigger(id, t.scope(), t.activation(), t.priority(), t.order(), t.time(), t.enabled()));
        }

        State before = ref.get();
        // bump version only if content differs
        boolean differs = !before.byId.equals(nextById);
        if (!differs)
            return;

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values()));

        ref.set(after);
        notifyListeners(after.snapshot);
    }

    @Override
    public Snapshot snapshot() {
        return ref.get().snapshot;
    }

    @Override
    public Optional<Trigger> get(String triggerId) {
        return Optional.ofNullable(ref.get().byId.get(triggerId));
    }

    @Override
    public boolean contains(String triggerId) {
        return ref.get().byId.containsKey(triggerId);
    }

    @Override
    public List<Trigger> list() {
        return List.copyOf(ref.get().byId.values());
    }

    @Override
    public void addListener(Consumer<Snapshot> onVersionChange) {
        if (onVersionChange != null)
            listeners.add(onVersionChange);
    }

    @Override
    public void removeListener(Consumer<Snapshot> onVersionChange) {
        if (onVersionChange != null)
            listeners.remove(onVersionChange);
    }

    public Set<String> candidatesForNode(EventType eventType, Iterable<String> labels) {
        Snapshot s = snapshot();
        IndexView ix = s.indexView();
        Map<String, Set<String>> eventActivations = ix.nodeIndex().getOrDefault(eventType, Map.of());
        Set<String> out = new LinkedHashSet<>();
        if (eventActivations.isEmpty()) {
            return Set.of();
        }
        for (String lbl : labels) {
            out.addAll(eventActivations.getOrDefault(lbl, Set.of()));
        }
        return out;
    }

    /**
     * Convenience: candidates for a relationship event given the relationship type.
     */
    public Set<String> candidatesForRelationship(EventType eventType, String relType) {
        Snapshot s = snapshot();
        IndexView ix = s.indexView();
        Map<String, Set<String>> eventActivations = ix.relIndex().getOrDefault(eventType, Map.of());
        Set<String> out = new LinkedHashSet<>();
        if (eventActivations.isEmpty()) {
            return Set.of();
        }
        out.addAll(eventActivations.getOrDefault(relType, Set.of()));
        return out;
    }

    /* ---------- Snapshot & IndexView implementations ---------- */

    private static final class SnapshotImpl implements Snapshot {
        private final long version;
        private final Map<String, Trigger> byId;
        private final IndexView indexView;

        SnapshotImpl(long version, Map<String, Trigger> byId, IndexView indexView) {
            this.version = version;
            this.byId = byId;
            this.indexView = indexView;
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Map<String, Trigger> byId() {
            return byId;
        }

        @Override
        public IndexView indexView() {
            return indexView;
        }
    }

    private static final class IndexViewImpl implements IndexView {
        private final Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> nodeIndex;
        private final Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> relIndex;
        private final TriggerRegistryInterface.PathMonitor pathMonitor;

        private IndexViewImpl(
                Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> nodeIndex,
                Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> relIndex,
                PathMonitor pathMonitor) {
            this.nodeIndex = nodeIndex;
            this.relIndex = relIndex;
            this.pathMonitor = pathMonitor;
        }

        static IndexViewImpl empty() {
            return new IndexViewImpl(
                    Collections.unmodifiableMap(new HashMap<>()),
                    Collections.unmodifiableMap(new HashMap<>()),
                    new EmptyPathMonitor());
        }

        @Override
        public Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> nodeIndex() {
            return nodeIndex;
        }

        @Override
        public Map<TriggerRegistryInterface.EventType, Map<String, Set<String>>> relIndex() {
            return relIndex;
        }

        @Override
        public PathMonitor pathMonitor() {
            return pathMonitor;
        }

    }

    // trivial implementation
    private static final class EmptyPathMonitor implements PathMonitor {

        @Override
        public Set<String> findMatchingTriggers(org.neo4j.graphdb.Transaction tx, String canonicalSignature) {
            return Set.of();
        }

    }

    protected abstract PathMonitor buildPathMonitor(Collection<TriggerRegistry.Trigger> triggers);

    /* ---------- Index building ---------- */

    protected IndexViewImpl buildIndexes(Collection<TriggerRegistry.Trigger> triggers) {
        Map<EventType, Map<String, Set<String>>> nodeIndex = new HashMap<>();
        Map<EventType, Map<String, Set<String>>> relIndex = new HashMap<>();
        Map<String, Set<String>> createEventMapNodes = new HashMap<>();
        Map<String, Set<String>> createEventMapRel = new HashMap<>();
        Map<String, Set<String>> deleteEventMapNode = new HashMap<>();
        Map<String, Set<String>> deleteEventMapRel = new HashMap<>();

        nodeIndex.put(EventType.ON_DELETE, deleteEventMapNode);
        nodeIndex.put(EventType.ON_CREATE, createEventMapNodes);
        relIndex.put(EventType.ON_DELETE, deleteEventMapRel);
        relIndex.put(EventType.ON_CREATE, createEventMapRel);

        for (Trigger t : triggers) {
            if (t == null)
                continue;
            String id = t.id();
            Scope scope = t.scope();
            Activation act = t.activation();

            if (scope == Scope.NODE && act instanceof NodeActivation na) {
                Set<String> labels = na.anyOfLabels();
                EventType ev = na.eventType();
                Map<String, Set<String>> eventMap = nodeIndex.get(ev);
                for (String lbl : labels) {
                    add(eventMap, lbl, id);
                }

            } else if (scope == Scope.RELATIONSHIP && act instanceof RelActivation ra) {
                String type = ra.type();
                EventType ev = ra.eventType();
                Map<String, Set<String>> eventMap = relIndex.get(ev);
                add(eventMap, type, id);
            }

            // IMPORTANT: PATH logic is not here – that’s what buildPathMonitor() is for
        }

        // ask subclass how to handle paths
        PathMonitor pathMonitor = buildPathMonitor(triggers);

        return new IndexViewImpl(
                freezeMapMap(nodeIndex),
                freezeMapMap(relIndex),
                pathMonitor);
    }

    private static <K> void add(Map<K, Set<String>> map, K key, String id) {
        map.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(id);
    }

    /** Freeze a leaf map like Map<String, Set<String>> */
    private static Map<String, Set<String>> freezeMap(Map<String, Set<String>> in) {
        Map<String, Set<String>> out = new LinkedHashMap<>(in.size());
        for (Map.Entry<String, Set<String>> e : in.entrySet()) {
            out.put(e.getKey(), Collections.unmodifiableSet(new LinkedHashSet<>(e.getValue())));
        }
        return Collections.unmodifiableMap(out);
    }

    /** Freeze a nested map like Map<EventType, Map<String, Set<String>>> */
    private static Map<EventType, Map<String, Set<String>>> freezeMapMap(
            Map<EventType, Map<String, Set<String>>> in) {
        Map<EventType, Map<String, Set<String>>> out = new EnumMap<>(EventType.class);
        for (Map.Entry<EventType, Map<String, Set<String>>> e : in.entrySet()) {
            // Freeze inner leaf map first
            Map<String, Set<String>> inner = e.getValue();
            Map<String, Set<String>> frozenInner = new LinkedHashMap<>(inner.size());
            for (Map.Entry<String, Set<String>> ie : inner.entrySet()) {
                frozenInner.put(ie.getKey(),
                        Collections.unmodifiableSet(new LinkedHashSet<>(ie.getValue())));
            }
            out.put(e.getKey(), Collections.unmodifiableMap(frozenInner));
        }
        return Collections.unmodifiableMap(out);
    }

    private void notifyListeners(Snapshot snap) {
        for (Consumer<Snapshot> c : listeners) {
            try {
                c.accept(snap);
            } catch (Exception ignore) {
                /* don't break others */ }
        }
    }
}
