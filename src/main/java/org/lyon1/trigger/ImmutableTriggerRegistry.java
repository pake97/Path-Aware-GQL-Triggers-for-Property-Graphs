package org.lyon1.trigger;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.lyon1.trigger.TriggerRegistry.EventType;
/**
 * Copy-on-write implementation of TriggerRegistry.
 * - Reads are lock-free (grab immutable Snapshot once and reuse).
 * - Writes rebuild a new immutable state and swap atomically.
 */
public final class ImmutableTriggerRegistry implements TriggerRegistry {

    /* ---------- Internal state ---------- */

    private static final class State {
        final long version;
        final Map<String, Trigger> byId;            // unmodifiable
        final IndexViewImpl indexView;              // unmodifiable sets inside
        final SnapshotImpl snapshot;                // immutable wrapper

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

    public ImmutableTriggerRegistry() {
        this.ref = new AtomicReference<>(new State(
                0L,
                Collections.unmodifiableMap(new LinkedHashMap<>()),
                IndexViewImpl.empty()
        ));
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
                trigger.enabled()
        );

        State before = ref.get();
        if (before.byId.containsKey(id)) {
            // Upsert semantics? The interface says "register" should add; if exists, we replace.
            // Change to error if you prefer strict "must be new".
        }

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.put(id, normalized);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values())
        );

        ref.set(after);
        notifyListeners(after.snapshot);
        return id;
    }

    @Override
    public boolean replace(Trigger trigger) {
        Objects.requireNonNull(trigger, "trigger");
        String id = trigger.id();
        if (id == null || id.isBlank()) return false;

        State before = ref.get();
        if (!before.byId.containsKey(id)) return false;

        Trigger normalized = new Trigger(
                id,
                trigger.scope(),
                trigger.activation(),
                trigger.priority(),
                trigger.enabled()
        );

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.put(id, normalized);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values())
        );

        ref.set(after);
        notifyListeners(after.snapshot);
        return true;
    }

    @Override
    public boolean enable(String triggerId, boolean enabled) {
        Objects.requireNonNull(triggerId, "triggerId");
        State before = ref.get();
        Trigger cur = before.byId.get(triggerId);
        if (cur == null || cur.enabled() == enabled) return false;

        Trigger updated = new Trigger(
                cur.id(), cur.scope(), cur.activation(), cur.priority(), enabled
        );

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.put(triggerId, updated);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values())
        );

        ref.set(after);
        notifyListeners(after.snapshot);
        return true;
    }

    @Override
    public boolean unregister(String triggerId) {
        Objects.requireNonNull(triggerId, "triggerId");
        State before = ref.get();
        if (!before.byId.containsKey(triggerId)) return false;

        Map<String, Trigger> nextById = new LinkedHashMap<>(before.byId);
        nextById.remove(triggerId);

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values())
        );

        ref.set(after);
        notifyListeners(after.snapshot);
        return true;
    }

    @Override
    public void replaceAll(Collection<Trigger> triggers) {
        Objects.requireNonNull(triggers, "triggers");
        Map<String, Trigger> nextById = new LinkedHashMap<>();
        for (Trigger t : triggers) {
            if (t == null) continue;
            String id = (t.id() != null && !t.id().isBlank()) ? t.id() : UUID.randomUUID().toString();
            nextById.put(id, new Trigger(id, t.scope(), t.activation(), t.priority(), t.enabled()));
        }

        State before = ref.get();
        // bump version only if content differs
        boolean differs = !before.byId.equals(nextById);
        if (!differs) return;

        State after = new State(
                before.version + 1,
                Collections.unmodifiableMap(nextById),
                buildIndexes(nextById.values())
        );

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
        if (onVersionChange != null) listeners.add(onVersionChange);
    }

    @Override
    public void removeListener(Consumer<Snapshot> onVersionChange) {
        if (onVersionChange != null) listeners.remove(onVersionChange);
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

        @Override public long version() { return version; }
        @Override public Map<String, Trigger> byId() { return byId; }
        @Override public IndexView indexView() { return indexView; }
    }

    private static final class IndexViewImpl implements IndexView {
        private final Map<String, Set<String>> nodeLabel;
        private final Map<String, Set<String>> relType;
        private final Map<EventType, Set<String>> globalNode;
        private final Map<EventType, Set<String>> globalRel;
        private final Map<String, Set<String>> pathSignature;

        private IndexViewImpl(
                Map<String, Set<String>> nodeLabel,
                Map<String, Set<String>> relType,
                Map<EventType, Set<String>> globalNode,
                Map<EventType, Set<String>> globalRel,
                Map<String, Set<String>> pathSignature) {
            this.nodeLabel = nodeLabel;
            this.relType = relType;
            this.globalNode = globalNode;
            this.globalRel = globalRel;
            this.pathSignature = pathSignature;
        }

        static IndexViewImpl empty() {
            return new IndexViewImpl(
                    Collections.unmodifiableMap(new HashMap<>()),
                    Collections.unmodifiableMap(new HashMap<>()),
                    Collections.unmodifiableMap(new EnumMap<>(EventType.class)),
                    Collections.unmodifiableMap(new EnumMap<>(EventType.class)),
                    Collections.unmodifiableMap(new HashMap<>())
            );
        }

        @Override public Map<String, Set<String>> nodeLabel() { return nodeLabel; }
        @Override public Map<String, Set<String>> relType() { return relType; }
        @Override public Map<EventType, Set<String>> globalNode() { return globalNode; }
        @Override public Map<EventType, Set<String>> globalRel() { return globalRel; }
        @Override public Map<String, Set<String>> pathSignature() { return pathSignature; }
    }

    /* ---------- Index building ---------- */

    private static IndexViewImpl buildIndexes(Collection<Trigger> triggers) {
        Map<String, Set<String>> nodeLabel = new HashMap<>();
        Map<String, Set<String>> relType = new HashMap<>();
        Map<EventType, Set<String>> globalNode = new EnumMap<>(EventType.class);
        Map<EventType, Set<String>> globalRel = new EnumMap<>(EventType.class);
        Map<String, Set<String>> pathSignature = new HashMap<>();

        for (Trigger t : triggers) {
            if (t == null) continue;
            String id = t.id();
            Scope scope = t.scope();
            Activation act = t.activation();

            if (scope == Scope.NODE && act instanceof NodeActivation na) {
                Set<String> labels = na.anyOfLabels();
                EventType ev = na.eventType();
                if (labels == null || labels.isEmpty()) {
                    add(globalNode, ev, id);
                } else {
                    for (String lbl : labels) add(nodeLabel, lbl, id);
                }
            } else if (scope == Scope.RELATIONSHIP && act instanceof RelActivation ra) {
                String type = ra.type();
                EventType ev = ra.eventType();
                if (type == null || type.isBlank()) {
                    add(globalRel, ev, id);
                } else {
                    add(relType, type, id);
                }
            } else if (scope == Scope.PATH && act instanceof PathActivation pa) {
                String sig = pa.canonicalSignature();
                if (sig != null && !sig.isBlank()) {
                    add(pathSignature, sig, id);
                }
            }
        }

        // Freeze to unmodifiable maps/sets
        return new IndexViewImpl(
                freeze(nodeLabel),
                freeze(relType),
                freezeEnum(globalNode),
                freezeEnum(globalRel),
                freeze(pathSignature)
        );
    }

    private static <K> void add(Map<K, Set<String>> map, K key, String id) {
        map.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(id);
    }

    private static <K extends Enum<K>> void add(Map<K, Set<String>> map, K key, String id) {
        map.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(id);
    }

    private static <K> Map<K, Set<String>> freeze(Map<K, Set<String>> in) {
        Map<K, Set<String>> out = new LinkedHashMap<>(in.size());
        for (Map.Entry<K, Set<String>> e : in.entrySet()) {
            out.put(e.getKey(), Collections.unmodifiableSet(new LinkedHashSet<>(e.getValue())));
        }
        return Collections.unmodifiableMap(out);
    }

    private static <K extends Enum<K>> Map<K, Set<String>> freezeEnum(Map<K, Set<String>> in) {
        Map<K, Set<String>> out = new EnumMap(EventType.class);
        for (Map.Entry<K, Set<String>> e : in.entrySet()) {
            out.put(e.getKey(), Collections.unmodifiableSet(new LinkedHashSet<>(e.getValue())));
        }
        return Collections.unmodifiableMap(out);
    }

    /* ---------- Listeners ---------- */

    private void notifyListeners(Snapshot snap) {
        for (Consumer<Snapshot> c : listeners) {
            try { c.accept(snap); } catch (Exception ignore) { /* don't break others */ }
        }
    }
}
