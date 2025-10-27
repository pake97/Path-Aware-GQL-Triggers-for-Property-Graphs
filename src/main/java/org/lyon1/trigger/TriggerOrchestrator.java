package org.lyon1.trigger;
import java.util.List;

interface TriggerOrchestrator {
    String register(Trigger t);        // returns id
    boolean unregister(String id);
    List<Trigger> list();
    void enable(String id, boolean enabled);
}
