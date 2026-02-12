package org.lyon1.trigger;

import org.neo4j.dbms.api.DatabaseManagementService;

import java.util.*;

import java.util.Collection;

public final class AutomatonTriggerRegistry extends TriggerRegistry {

    public AutomatonTriggerRegistry(DatabaseManagementService dbms) {
        super(dbms);
    }

    @Override
    protected PathMonitor buildPathMonitor(Collection<TriggerRegistryInterface.Trigger> triggers) {
        // Whatever you need to build your automaton-based path monitor.
        // This is the ONLY part that differs for this subclass.
        return AutomatonPathMonitor.fromTriggers(triggers);
    }

    public Set<String> candidatesForPath(String path) {
        return Set.of();
    }
}
