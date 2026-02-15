package org.lyon1.trigger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lyon1.Listener;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.logging.NullLog;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AutomatonPathTriggerTest {

    private Neo4j neo4j;
    private DatabaseManagementService dbms;
    private GraphDatabaseService db;
    private InMemoryOrchestrator orchestrator;
    private TriggerRegistry registry;

    @BeforeAll
    void setup() {
        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .build();
        dbms = neo4j.databaseManagementService();
        db = neo4j.defaultDatabaseService();

        registry = TriggerRegistryFactory.create(TriggerType.AUTOMATON, dbms, NullLog.getInstance());
        orchestrator = new InMemoryOrchestrator(registry);

        // Pass essential components to the listener
        Listener listener = new Listener(NullLog.getInstance(), orchestrator, registry);
        dbms.registerTransactionEventListener("neo4j", listener);
    }

    @AfterAll
    void tearDown() {
        if (neo4j != null) {
            neo4j.close();
        }
    }

    @Test
    void testPathTriggerFiresOnCreation() {
        AtomicInteger fires = new AtomicInteger(0);

        // Path to monitor: (:Person)-[:WORKS_AT]->(:Company)
        TriggerRegistryInterface.PathActivation activation = new TriggerRegistryInterface.PathActivation(
                "(:Person)-[:WORKS_AT]->(:Company)", 0, TriggerRegistryInterface.EventType.ON_CREATE);

        orchestrator.register(new FullTrigger(
                "path-trigger-1",
                TriggerRegistryInterface.Scope.PATH,
                activation,
                100,
                true,
                ctx -> true,
                committed -> fires.incrementAndGet(),
                TriggerRegistryInterface.Time.AFTER_COMMIT,
                0));

        // 1. Create nodes
        try (Transaction tx = db.beginTx()) {
            tx.createNode(Label.label("Person"));
            tx.createNode(Label.label("Company"));
            tx.commit();
        }
        assertEquals(0, fires.get(), "Trigger should not fire for individual node creation");

        // 2. Create relationship that completes the path
        try (Transaction tx = db.beginTx()) {
            var person = tx.findNodes(Label.label("Person")).next();
            var company = tx.findNodes(Label.label("Company")).next();
            person.createRelationshipTo(company, RelationshipType.withName("WORKS_AT"));
            tx.commit();
        }

        assertEquals(1, fires.get(), "Trigger should fire exactly once when the path is completed");
    }
}
