package org.lyon1.trigger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lyon1.Listener;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IndexPathTriggerPerformanceTest {

    private Neo4j neo4j;
    private DatabaseManagementService dbms;
    private GraphDatabaseService db;
    private InMemoryOrchestrator orchestrator;
    private TriggerRegistry registry;
    private Listener listener;

    private static final Label PERSON = Label.label("Person");
    private static final Label ACCOUNT = Label.label("Account");
    private static final Label LOAN = Label.label("Loan");
    private static final RelationshipType OWN = RelationshipType.withName("OWN");
    private static final RelationshipType DEPOSIT = RelationshipType.withName("DEPOSIT");

    @BeforeAll
    void setup() {
        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .build();
        dbms = neo4j.databaseManagementService();
        db = neo4j.defaultDatabaseService();

        org.neo4j.logging.Log consoleLog = new org.neo4j.logging.Log() {
            @Override
            public boolean isDebugEnabled() {
                return true;
            }

            @Override
            public void debug(String message) {
                System.out.println("DEBUG: " + message);
            }

            @Override
            public void debug(String message, Throwable throwable) {
                System.out.println("DEBUG: " + message);
                throwable.printStackTrace();
            }

            @Override
            public void debug(String format, Object... params) {
                try {
                    System.out.println("DEBUG: " + String.format(format.replace("{}", "%s"), params));
                } catch (Exception e) {
                }
            }

            @Override
            public void info(String message) {
                System.out.println("INFO: " + message);
            }

            @Override
            public void info(String message, Throwable throwable) {
                System.out.println("INFO: " + message);
                throwable.printStackTrace();
            }

            @Override
            public void info(String format, Object... params) {
                try {
                    System.out.println("INFO: " + String.format(format.replace("{}", "%s"), params));
                } catch (Exception e) {
                }
            }

            @Override
            public void warn(String message) {
                System.out.println("WARN: " + message);
            }

            @Override
            public void warn(String message, Throwable throwable) {
                System.out.println("WARN: " + message);
                throwable.printStackTrace();
            }

            @Override
            public void warn(String format, Object... params) {
                try {
                    System.out.println("WARN: " + String.format(format.replace("{}", "%s"), params));
                } catch (Exception e) {
                }
            }

            @Override
            public void error(String message) {
                System.err.println("ERROR: " + message);
            }

            @Override
            public void error(String message, Throwable throwable) {
                System.err.println("ERROR: " + message);
                throwable.printStackTrace();
            }

            @Override
            public void error(String format, Object... params) {
                try {
                    System.err.println("ERROR: " + String.format(format.replace("{}", "%s"), params));
                } catch (Exception e) {
                }
            }
        };

        // Use INDEX type
        registry = TriggerRegistryFactory.create(TriggerType.INDEX, dbms, consoleLog);
        orchestrator = new InMemoryOrchestrator(registry);

        listener = new Listener(consoleLog, orchestrator, registry);
        dbms.registerTransactionEventListener("neo4j", listener);
    }

    @AfterAll
    void tearDown() {
        if (neo4j != null) {
            neo4j.close();
        }
    }

    private void clearDatabase() {
        try (Transaction tx = db.beginTx()) {
            tx.execute("MATCH (n) DETACH DELETE n");
            tx.commit();
        }
    }

    private void materializeGraph(int numPersons, int accountsPerPerson, int loansPerAccount) {
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < numPersons; i++) {
                Node person = tx.createNode(PERSON);
                person.setProperty("pid", i);
                for (int j = 0; j < accountsPerPerson; j++) {
                    Node account = tx.createNode(ACCOUNT);
                    int aid = i * 1000 + j;
                    account.setProperty("aid", aid);
                    person.createRelationshipTo(account, OWN);

                    for (int k = 0; k < loansPerAccount; k++) {
                        Node loan = tx.createNode(LOAN);
                        loan.setProperty("lid", i * 1000000 + j * 1000 + k);
                        loan.setProperty("target_aid", aid);
                    }
                }
            }
            tx.commit();
        }
    }

    @Test
    void runPerformanceTest() {
        int[] fanOuts = { 2, 5 };
        int numPersons = 3;

        System.out.println("Fan-Out | Avg Latency (ms) | Total Time With (ms) | Total Time Without (ms) | Memory (MB)");
        System.out.println("--------|------------------|----------------------|-------------------------|------------");

        for (int fanOut : fanOuts) {
            runTestIteration(numPersons, fanOut, fanOut);
        }
    }

    private void runTestIteration(int numPersons, int accountsPerPerson, int loansPerAccount) {
        clearDatabase();

        TriggerRegistryInterface.PathActivation activation = new TriggerRegistryInterface.PathActivation(
                "(:Person)-[:OWN]->(:Account)<-[:DEPOSIT]-(:Loan)", 0, TriggerRegistryInterface.EventType.ON_CREATE);

        AtomicLong lastTriggerDetectedTime = new AtomicLong(0);
        AtomicLong triggerCount = new AtomicLong(0);

        String triggerId = orchestrator.register(new FullTrigger(
                "index-perf-trigger-" + accountsPerPerson,
                TriggerRegistryInterface.Scope.PATH,
                activation,
                100,
                true,
                ctx -> true,
                committed -> {
                    lastTriggerDetectedTime.set(System.nanoTime());
                    triggerCount.incrementAndGet();
                },
                TriggerRegistryInterface.Time.AFTER_COMMIT,
                0));

        materializeGraph(numPersons, accountsPerPerson, loansPerAccount);

        // 1. Measure with Trigger
        List<Long> latencies = new ArrayList<>();
        long startWith = System.currentTimeMillis();
        performInsertions(latencies, lastTriggerDetectedTime);
        long endWith = System.currentTimeMillis();

        System.out.println("Trigger firing count for fan-out " + accountsPerPerson + ": " + triggerCount.get());

        System.gc();
        long memoryAfter = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);

        // 2. Measure without Trigger
        orchestrator.unregister(triggerId);
        clearDatabase();
        materializeGraph(numPersons, accountsPerPerson, loansPerAccount);

        long startWithout = System.currentTimeMillis();
        performInsertions(null, null);
        long endWithout = System.currentTimeMillis();

        double avgLatency = 0;
        if (!latencies.isEmpty()) {
            avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
        }

        System.out.printf("%7d | %16.4f | %20d | %23d | %10d\n",
                accountsPerPerson, avgLatency, (endWith - startWith), (endWithout - startWithout), memoryAfter);
    }

    private void performInsertions(List<Long> latencies, AtomicLong lastTriggerDetectedTime) {
        List<long[]> tasks = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            tx.findNodes(LOAN).forEachRemaining(loan -> {
                long targetAid = ((Number) loan.getProperty("target_aid")).longValue();
                tasks.add(new long[] { loan.getId(), targetAid });
            });
            tx.commit();
        }

        for (long[] task : tasks) {
            long loanId = task[0];
            long targetAid = task[1];

            long startTime = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                Node loan = tx.getNodeById(loanId);
                Node account = tx.findNode(ACCOUNT, "aid", (int) targetAid);
                if (loan != null && account != null) {
                    loan.createRelationshipTo(account, DEPOSIT);
                }
                tx.commit();
            }
            if (latencies != null && lastTriggerDetectedTime != null) {
                long detectedTime = lastTriggerDetectedTime.get();
                if (detectedTime > startTime) {
                    latencies.add(detectedTime - startTime);
                }
            }
        }
    }
}
