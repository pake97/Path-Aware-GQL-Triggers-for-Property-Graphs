package org.lyon1.trigger;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.Log;

import java.util.*;

import java.util.Collection;

public final class AutomatonTriggerRegistry extends TriggerRegistry {

    public AutomatonTriggerRegistry(DatabaseManagementService dbms, Log log) {
        super(dbms, log);
    }

    @Override
    protected PathMonitor buildPathMonitor(Collection<TriggerRegistry.Trigger> triggers) {
        // Whatever you need to build your automaton-based path monitor.
        // This is the ONLY part that differs for this subclass.
        return AutomatonPathMonitor.fromTriggers(triggers, neoLog);
    }

    public Set<String> candidatesForPath(String path) {
        return Set.of();
    }
}
