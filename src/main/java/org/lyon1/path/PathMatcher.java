package org.lyon1.path;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;

import java.util.Set;

/**
 * Path matching contract: given the tx delta, return canonical path signatures
 * that actually materialize because of this transaction (create/delete).
 * You can keep this cheap by exploring only neighborhoods around changed elements.
 */
public interface PathMatcher {
    Set<String> findCanonicalSignatures(TransactionData data, GraphDatabaseService db);
}
