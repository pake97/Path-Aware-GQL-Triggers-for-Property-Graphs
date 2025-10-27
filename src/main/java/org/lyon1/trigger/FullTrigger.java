package org.lyon1.trigger;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.lyon1.trigger.TxContext;
import org.lyon1.trigger.CommittedContext;


/** Full trigger definition (includes predicate/action). */
public final class FullTrigger {
    private final String id;
    private final TriggerRegistry.Scope scope;
    private final TriggerRegistry.Activation activation;
    private final int priority;
    private final boolean enabled;
    private final Predicate<TxContext> predicate;
    private final Consumer<CommittedContext> action;

    public FullTrigger(String id,
                       TriggerRegistry.Scope scope,
                       TriggerRegistry.Activation activation,
                       int priority,
                       boolean enabled,
                       Predicate<TxContext> predicate,
                       Consumer<CommittedContext> action) {
        this.id = id;
        this.scope = Objects.requireNonNull(scope, "scope");
        this.activation = Objects.requireNonNull(activation, "activation");
        this.priority = priority;
        this.enabled = enabled;
        this.predicate = Objects.requireNonNull(predicate, "predicate");
        this.action = Objects.requireNonNull(action, "action");
    }

    public String id() { return id; }
    public TriggerRegistry.Scope scope() { return scope; }
    public TriggerRegistry.Activation activation() { return activation; }
    public int priority() { return priority; }
    public boolean enabled() { return enabled; }
    public Predicate<TxContext> predicate() { return predicate; }
    public Consumer<CommittedContext> action() { return action; }

    public FullTrigger withId(String newId) {
        return new FullTrigger(newId, scope, activation, priority, enabled, predicate, action);
    }
    public FullTrigger withEnabled(boolean e) {
        return new FullTrigger(id, scope, activation, priority, e, predicate, action);
    }
    public FullTrigger withPriority(int p) {
        return new FullTrigger(id, scope, activation, p, enabled, predicate, action);
    }
}