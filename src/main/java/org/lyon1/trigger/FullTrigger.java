package org.lyon1.trigger;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;


/** Full trigger definition (includes predicate/action). */
public final class FullTrigger {
    private final String id;
    private final TriggerRegistryInterface.Scope scope;
    private final TriggerRegistryInterface.Activation activation;
    private final int priority;
    private final boolean enabled;
    private final Predicate<TxContext> predicate;
    private final Consumer<CommittedContext> action;
    private final int order;
    private final TriggerRegistryInterface.Time time;

    public FullTrigger(String id,
                       TriggerRegistryInterface.Scope scope,
                       TriggerRegistryInterface.Activation activation,
                       int priority,
                       boolean enabled,
                       Predicate<TxContext> predicate,
                       Consumer<CommittedContext> action,
                       TriggerRegistryInterface.Time time,
                       int order
                       ) {
        this.id = id;
        this.scope = Objects.requireNonNull(scope, "scope");
        this.activation = Objects.requireNonNull(activation, "activation");
        this.priority = priority;
        this.enabled = enabled;
        this.predicate = Objects.requireNonNull(predicate, "predicate");
        this.action = Objects.requireNonNull(action, "action");
        this.time = Objects.requireNonNull(time, "time");
        this.order = order;

    }

    public String id() { return id; }
    public TriggerRegistryInterface.Scope scope() { return scope; }
    public TriggerRegistryInterface.Activation activation() { return activation; }
    public int priority() { return priority; }
    public boolean enabled() { return enabled; }
    public Predicate<TxContext> predicate() { return predicate; }
    public Consumer<CommittedContext> action() { return action; }
    public TriggerRegistryInterface.Time time() { return time; }
    public int order() { return order; }

    public FullTrigger withId(String newId) {
        return new FullTrigger(newId, scope, activation, priority, enabled, predicate, action, time, order);
    }
    public FullTrigger withEnabled(boolean e) {
        return new FullTrigger(id, scope, activation, priority, e, predicate, action, time, order);
    }
    public FullTrigger withPriority(int p) {
        return new FullTrigger(id, scope, activation, p, enabled, predicate, action, time, order);
    }
}