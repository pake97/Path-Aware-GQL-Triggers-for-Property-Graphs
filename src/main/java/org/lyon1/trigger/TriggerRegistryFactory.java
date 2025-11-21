package org.lyon1.trigger;

import org.neo4j.dbms.api.DatabaseManagementService;

public final class TriggerRegistryFactory {

    public TriggerRegistryFactory() {
        // utility class – no instances
    }

    public static TriggerRegistry create(TriggerType type, DatabaseManagementService dbms) {
        return switch (type) {
            case AUTOMATON -> new AutomatonTriggerRegistry(dbms);
            case INDEX     -> new IndexTriggerRegistry(dbms);
        };
    }
}
