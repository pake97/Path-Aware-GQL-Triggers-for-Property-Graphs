package org.lyon1.trigger;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Orchestrator:
 *  - Owns the executable FullTrigger (with predicate/action)
 *  - Mirrors a slim TriggerRegistry.Trigger for indexing & candidate lookup
 *  - All mutations are copy-on-write; reads are lock-free
 */
public final class InMemoryOrchestrator {


    /* -------------------- Storage & registry wiring -------------------- */

    private final TriggerRegistry registry;
    /** id -> FullTrigger; copy-on-write for atomic reads */
    private final AtomicReference<Map<String, FullTrigger>> full = new AtomicReference<>(Map.of());

    public InMemoryOrchestrator(TriggerRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /* -------------------- CRUD -------------------- */

    /** Register a trigger; returns the id (generated if null/blank). */
    public String register(FullTrigger t) {
        Objects.requireNonNull(t, "trigger");
        final String id = (t.id() != null && !t.id().isBlank()) ? t.id() : UUID.randomUUID().toString();

        // 1) write into registry (indexed view)
        var slim = new TriggerRegistryInterface.Trigger(
                id, t.scope(), t.activation(), t.priority(),  t.order(),t.time(), t.enabled()
                );
        registry.register(slim);

        // 2) store executable definition (copy-on-write)
        putFull(t.withId(id));
        return id;
    }

    /** Replace the whole definition for an existing id. Returns false if not found. */
    public boolean replace(FullTrigger t) {
        Objects.requireNonNull(t, "trigger");
        if (t.id() == null || t.id().isBlank()) return false;

        // upsert slim into registry (only if exists per interface)
        boolean ok = registry.replace(new TriggerRegistryInterface.Trigger(
                t.id(), t.scope(), t.activation(), t.priority(),  t.order(),t.time(), t.enabled()
        ));
        if (!ok) return false;

        // replace full
        replaceFull(t);
        return true;
    }

    /** Enable/disable; returns false if id unknown or no change. */
    public boolean enable(String id, boolean enabled) {
        Objects.requireNonNull(id, "id");
        boolean changed = registry.enable(id, enabled);
        if (!changed) return false;
        // mirror in full map
        updateFull(id, ft -> ft.withEnabled(enabled));
        return true;
    }

    /** Unregister; returns false if not found. */
    public boolean unregister(String id) {
        Objects.requireNonNull(id, "id");
        boolean ok = registry.unregister(id);
        if (!ok) return false;
        removeFull(id);
        return true;
    }

    /** Replace all at once (copy-on-write swap). */
    public void replaceAll(Collection<FullTrigger> triggers) {
        Objects.requireNonNull(triggers, "triggers");

        // build slim set first
        List<TriggerRegistryInterface.Trigger> slim = new ArrayList<>(triggers.size());
        Map<String, FullTrigger> next = new LinkedHashMap<>();
        for (FullTrigger ft : triggers) {
            if (ft == null) continue;
            String id = (ft.id() != null && !ft.id().isBlank()) ? ft.id() : UUID.randomUUID().toString();
            slim.add(new TriggerRegistryInterface.Trigger(id, ft.scope(), ft.activation(), ft.priority(),  ft.order(),ft.time(), ft.enabled()));
            next.put(id, ft.withId(id));
        }

        registry.replaceAll(slim);
        full.set(Collections.unmodifiableMap(next));
    }

    /* -------------------- Reads (for your Listener) -------------------- */

    public List<FullTrigger> list() {
        return List.copyOf(full.get().values());
    }

    public Optional<FullTrigger> get(String id) {
        return Optional.ofNullable(full.get().get(id));
    }

    public TriggerRegistryInterface.Snapshot snapshot() {
        return registry.snapshot();
    }

    public TriggerRegistry registry() {
        return registry;
    }

    /* -------------------- Internal copy-on-write helpers -------------------- */

    private void putFull(FullTrigger ft) {
        while (true) {
            Map<String, FullTrigger> cur = full.get();
            Map<String, FullTrigger> next = new LinkedHashMap<>(cur);
            next.put(ft.id(), ft);
            if (full.compareAndSet(cur, Collections.unmodifiableMap(next))) return;
        }
    }

    private void replaceFull(FullTrigger ft) {
        while (true) {
            Map<String, FullTrigger> cur = full.get();
            if (!cur.containsKey(ft.id())) return;
            Map<String, FullTrigger> next = new LinkedHashMap<>(cur);
            next.put(ft.id(), ft);
            if (full.compareAndSet(cur, Collections.unmodifiableMap(next))) return;
        }
    }

    private void updateFull(String id, java.util.function.UnaryOperator<FullTrigger> fn) {
        while (true) {
            Map<String, FullTrigger> cur = full.get();
            FullTrigger old = cur.get(id);
            if (old == null) return;
            FullTrigger neu = fn.apply(old);
            if (neu == null) return;
            Map<String, FullTrigger> next = new LinkedHashMap<>(cur);
            next.put(id, neu);
            if (full.compareAndSet(cur, Collections.unmodifiableMap(next))) return;
        }
    }

    private void removeFull(String id) {
        while (true) {
            Map<String, FullTrigger> cur = full.get();
            if (!cur.containsKey(id)) return;
            Map<String, FullTrigger> next = new LinkedHashMap<>(cur);
            next.remove(id);
            if (full.compareAndSet(cur, Collections.unmodifiableMap(next))) return;
        }
    }
}
