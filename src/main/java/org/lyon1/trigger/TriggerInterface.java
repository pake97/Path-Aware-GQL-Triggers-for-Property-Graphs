package org.lyon1.trigger;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.lyon1.trigger.TxContext;





enum EventType { ON_CREATE, ON_DELETE }
enum Scope { NODE, RELATIONSHIP, PATH }

sealed interface Activation permits NodeActivation, RelActivation, PathActivation {}

record NodeActivation(
        Set<String> anyOfLabels,          // labels OR
        Map<String,Object> propertyEq,    // optional fast prefilter
        EventType eventType
) implements Activation {}

record RelActivation(
        String type,
        Set<String> startLabels,          // optional
        Set<String> endLabels,            // optional
        EventType eventType
) implements Activation {}

// Keep paths bounded-length. Canonicalize pattern (see below)
record PathActivation(
        String canonicalSignature,        // e.g. "(A)-[T]->(B)<-[U]-(C)"
        int maxLen,                       // if you support variable
        EventType eventType
) implements Activation {}

