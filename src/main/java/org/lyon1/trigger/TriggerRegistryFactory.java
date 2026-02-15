package org.lyon1.trigger;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.Log;

public final class TriggerRegistryFactory {

    public TriggerRegistryFactory() {
        // utility class – no instances
    }

    public static TriggerRegistry create(TriggerType type, DatabaseManagementService dbms, Log log) {
        return switch (type) {
            case AUTOMATON -> new AutomatonTriggerRegistry(dbms, log);
            case INDEX -> new IndexTriggerRegistry(dbms, log);
        };
    }
}
