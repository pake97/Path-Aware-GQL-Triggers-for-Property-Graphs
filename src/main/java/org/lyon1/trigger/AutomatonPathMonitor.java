package org.lyon1.trigger;

import java.util.Collection;
import java.util.Set;

public final class AutomatonPathMonitor implements TriggerRegistryInterface.PathMonitor {

    private AutomatonPathMonitor() {
    }

    public static AutomatonPathMonitor fromTriggers(Collection<TriggerRegistryInterface.Trigger> triggers) {
        return new AutomatonPathMonitor();
    }

    @Override
    public Set<String> findMatchingTriggers(String canonicalSignature) {
        return Set.of();
    }

}
