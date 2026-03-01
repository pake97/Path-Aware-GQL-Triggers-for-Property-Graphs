package org.lyon1.trigger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lyon1.Listener;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.logging.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Experiment2Testb {

    public static class QueryResult {
        public double avgTxMs;
        public double stdDevTx;
        public double avgActMs;
        public double stdDevAct;
        public double avgMem;
        public double stdDevMem;
        public double overhead;

        public QueryResult(double avgTxMs, double stdDevTx, double avgActMs, double stdDevAct, double avgMem,
                double stdDevMem, double overhead) {
            this.avgTxMs = avgTxMs;
            this.stdDevTx = stdDevTx;
            this.avgActMs = avgActMs;
            this.stdDevAct = stdDevAct;
            this.avgMem = avgMem;
            this.stdDevMem = stdDevMem;
            this.overhead = overhead;
        }
    }

    private Neo4j neo4j;
    private DatabaseManagementService dbms;
    private GraphDatabaseService db;
    private InMemoryOrchestrator orchestrator;
    private TriggerRegistry registry;
    private Listener listener;
    private Log log;

    private static final String DATA_PATH = "src/test/resources/sf0.1/";
    private static final String QUERIES_PATH = "src/test/resources/queries/";
    private static final String PATTERN = "(:Person)-[:guarantee]->(:Person)-[:guarantee]->(:Person)";

    @BeforeAll
    void setup() {
        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .build();
        dbms = neo4j.databaseManagementService();
        db = neo4j.defaultDatabaseService();

        log = new Log() {
            @Override
            public boolean isDebugEnabled() {
                return false;
            }

            @Override
            public void debug(String message) {
            }

            @Override
            public void debug(String message, Throwable throwable) {
            }

            @Override
            public void debug(String format, Object... params) {
            }

            @Override
            public void info(String message) {
            }

            @Override
            public void info(String message, Throwable throwable) {
            }

            @Override
            public void info(String format, Object... params) {
            }

            @Override
            public void warn(String message) {
            }

            @Override
            public void warn(String message, Throwable throwable) {
            }

            @Override
            public void warn(String format, Object... params) {
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
                System.err.printf("ERROR: " + format + "\n", params);
            }
        };
    }

    @AfterAll
    void tearDown() {
        if (neo4j != null) {
            neo4j.close();
        }
    }

    private void switchRegistry(TriggerType type) {
        if (listener != null) {
            dbms.unregisterTransactionEventListener("neo4j", listener);
        }
        registry = TriggerRegistryFactory.create(type, dbms, log);
        orchestrator = new InMemoryOrchestrator(registry);
        listener = new Listener(log, orchestrator, registry);
        dbms.registerTransactionEventListener("neo4j", listener);
    }

    private void clearDatabase() {
        try (Transaction tx = db.beginTx()) {
            tx.execute("MATCH (n) DETACH DELETE n");
            tx.commit();
        }
    }

    private String loadQuery(String filename) throws IOException {
        return new String(Files.readAllBytes(Paths.get(QUERIES_PATH + filename))).trim();
    }

    private List<Map<String, Object>> loadCsv(String filename, String[] expectedKeys, String[] csvHeaders)
            throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(DATA_PATH + filename))) {
            String line = br.readLine();
            if (line == null)
                return data;
            String[] actualHeaders = line.split("\\|");
            Map<String, Integer> headerIndex = new HashMap<>();
            for (int i = 0; i < actualHeaders.length; i++)
                headerIndex.put(actualHeaders[i], i);

            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|");
                Map<String, Object> params = new HashMap<>();
                for (int i = 0; i < expectedKeys.length; i++) {
                    String csvKey = csvHeaders[i];
                    String paramKey = expectedKeys[i];
                    if (headerIndex.containsKey(csvKey)) {
                        String val = values[headerIndex.get(csvKey)];
                        if (val.equals("true") || val.equals("false"))
                            params.put(paramKey, Boolean.parseBoolean(val));
                        else {
                            try {
                                params.put(paramKey, Long.parseLong(val));
                            } catch (NumberFormatException e) {
                                try {
                                    params.put(paramKey, Double.parseDouble(val));
                                } catch (NumberFormatException e2) {
                                    params.put(paramKey, val);
                                }
                            }
                        }
                    }
                }
                data.add(params);
            }
        }
        return data;
    }

    @Test
    void runExperiment2() throws IOException {
        System.out.println("Experiment 2b: Performance Comparison (Baseline vs INDEX vs PATH)");
        System.out.println("Trigger Pattern: " + PATTERN);
        System.out.println();
        System.out.println(
                "Query | Mode | Count | Avg Tx Time (ms) | StdDev Time | Avg Act Latency (ms) | StdDev Latency | Activations | Overhead (%) | Avg Mem (MB) | StdDev Mem");
        System.out.println(
                "------|------|-------|------------------|-------------|----------------------|----------------|-------------|--------------|--------------|-----------");

        // 1. Warmup
        System.err.println("--- Starting Warmup ---");
        switchRegistry(TriggerType.INDEX);
        clearDatabase();
        runFullSequence(false, null, null, true);
        runFullSequence(true, null, null, true);

        switchRegistry(TriggerType.AUTOMATON);
        clearDatabase();
        runFullSequence(true, null, null, true);

        // 2. Baseline
        System.err.println("--- Starting Baseline ---");
        clearDatabase();
        Map<String, QueryResult> baselineResults = runFullSequence(false, null, null, false);
        long baselineMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // 3. Index
        System.err.println("--- Starting Index Phase ---");
        switchRegistry(TriggerType.INDEX);
        Map<String, QueryResult> indexResults = runWithTrigger("INDEX");
        long indexMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // 4. Path (Automaton)
        System.err.println("--- Starting Path Phase ---");
        switchRegistry(TriggerType.AUTOMATON);
        Map<String, QueryResult> pathResults = runWithTrigger("PATH");
        long pathMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // Summary Line for tw-10.cypher
        QueryResult idxRes = indexResults.get("tw-10.cypher");
        QueryResult pathRes = pathResults.get("tw-10.cypher");

        if (idxRes != null && pathRes != null) {
            System.out.println();
            System.out.printf("Summary (tw-10): %.4f (%.4f) %.4f (%.4f) %.2f %.2f %.4f (%.4f) %.4f (%.4f)\n",
                    idxRes.avgActMs, idxRes.stdDevAct,
                    pathRes.avgActMs, pathRes.stdDevAct,
                    idxRes.overhead, pathRes.overhead,
                    idxRes.avgMem, idxRes.stdDevMem,
                    pathRes.avgMem, pathRes.stdDevMem);
        }

        System.out.printf("Absolute Heap Memory: Baseline: %.2f MB, Index: %.2f MB, Automaton: %.2f MB\n",
                baselineMem / (1024.0 * 1024.0),
                indexMem / (1024.0 * 1024.0),
                pathMem / (1024.0 * 1024.0));
    }

    @Test
    void runExperiment2WithCascading() throws IOException {
        System.out.println("Experiment 2 (CASCADING): Transitive Closure Performance (Baseline vs INDEX vs PATH)");
        System.out.println("Trigger Pattern: " + PATTERN);
        System.out.println();
        System.out.println(
                "Query | Mode | Count | Avg Tx Time (ms) | StdDev Time | Avg Act Latency (ms) | StdDev Latency | Activations | Overhead (%) | Avg Mem (MB) | StdDev Mem");
        System.out.println(
                "------|------|-------|------------------|-------------|----------------------|----------------|-------------|--------------|--------------|-----------");

        // 1. Warmup
        System.err.println("--- Starting Warmup (Cascading) ---");
        switchRegistry(TriggerType.INDEX);
        clearDatabase();
        runFullSequence(false, null, null, true);
        runWithCascadingTrigger("WARMUP-INDEX");

        switchRegistry(TriggerType.AUTOMATON);
        clearDatabase();
        runWithCascadingTrigger("WARMUP-PATH");

        // 2. Baseline
        System.err.println("--- Starting Baseline ---");
        clearDatabase();
        Map<String, QueryResult> baselineResults = runFullSequence(false, null, null, false);
        long baselineMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // 3. Index
        System.err.println("--- Starting Index Phase (Cascading) ---");
        switchRegistry(TriggerType.INDEX);
        Map<String, QueryResult> indexResults = runWithCascadingTrigger("INDEX");
        long indexMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // 4. Path (Automaton)
        System.err.println("--- Starting Path Phase (Cascading) ---");
        switchRegistry(TriggerType.AUTOMATON);
        Map<String, QueryResult> pathResults = runWithCascadingTrigger("PATH");
        long pathMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        // Summary Line for tw-10.cypher
        QueryResult idxRes = indexResults.get("tw-10.cypher");
        QueryResult pathRes = pathResults.get("tw-10.cypher");

        if (idxRes != null && pathRes != null) {
            System.out.println();
            System.out.printf("Summary (tw-10): %.4f (%.4f) %.4f (%.4f) %.2f %.2f %.4f (%.4f) %.4f (%.4f)\n",
                    idxRes.avgActMs, idxRes.stdDevAct,
                    pathRes.avgActMs, pathRes.stdDevAct,
                    idxRes.overhead, pathRes.overhead,
                    idxRes.avgMem, idxRes.stdDevMem,
                    pathRes.avgMem, pathRes.stdDevMem);
        }

        System.out.printf("Absolute Heap Memory: Baseline: %.2f MB, Index: %.2f MB, Automaton: %.2f MB\n",
                baselineMem / (1024.0 * 1024.0),
                indexMem / (1024.0 * 1024.0),
                pathMem / (1024.0 * 1024.0));
    }

    @Test
    void testCascadingTriggers() throws IOException {
        System.out.println("--- Starting Cascading Triggers Test ---");
        switchRegistry(TriggerType.AUTOMATON);
        clearDatabase();

        AtomicLong cascadingActivations = new AtomicLong(0);

        // 1. Materialization Trigger:
        orchestrator.register(new FullTrigger(
                "materialization-trigger",
                TriggerRegistryInterface.Scope.PATH,
                new TriggerRegistryInterface.PathActivation(PATTERN, 2, TriggerRegistryInterface.EventType.ON_CREATE),
                1,
                true,
                ctx -> true,
                committed -> {
                    for (TriggerRegistryInterface.PathMatch match : committed.matches()) {
                        String startNodeId = match.nodeIds().get(0);
                        String endNodeId = match.nodeIds().get(match.nodeIds().size() - 1);
                        System.out.println("MATERIALIZING: Creating indirectGuarantee between " + startNodeId + " and "
                                + endNodeId);
                        java.util.concurrent.CompletableFuture.runAsync(() -> {
                            try (Transaction tx = committed.db().beginTx()) {
                                Node start = tx.getNodeByElementId(startNodeId);
                                Node end = tx.getNodeByElementId(endNodeId);
                                start.createRelationshipTo(end, RelationshipType.withName("indirectGuarantee"));
                                tx.commit();
                            } catch (Exception e) {
                                System.err.println("Materialization failed: " + e.getMessage());
                            }
                        });
                    }
                },
                TriggerRegistryInterface.Time.AFTER_COMMIT,
                1));

        // 2. Watch Trigger:
        orchestrator.register(new FullTrigger(
                "watch-indirect",
                TriggerRegistryInterface.Scope.RELATIONSHIP,
                new TriggerRegistryInterface.RelActivation("indirectGuarantee", Collections.emptySet(),
                        Collections.emptySet(), TriggerRegistryInterface.EventType.ON_CREATE),
                1,
                true,
                ctx -> true,
                committed -> {
                    System.out.println("CASCADE SUCCESS: indirectGuarantee detected in second trigger!");
                    cascadingActivations.incrementAndGet();
                },
                TriggerRegistryInterface.Time.AFTER_COMMIT,
                2));

        // 3. Execution
        try (Transaction tx = db.beginTx()) {
            Node p1 = tx.createNode(Label.label("Person"));
            Node p2 = tx.createNode(Label.label("Person"));
            Node p3 = tx.createNode(Label.label("Person"));
            p1.createRelationshipTo(p2, RelationshipType.withName("guarantee"));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node p2 = tx.execute("MATCH (p:Person) RETURN p SKIP 1 LIMIT 1").<Node>columnAs("p").next();
            Node p3 = tx.execute("MATCH (p:Person) RETURN p SKIP 2 LIMIT 1").<Node>columnAs("p").next();
            p2.createRelationshipTo(p3, RelationshipType.withName("guarantee"));
            tx.commit();
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        System.out.println("Total cascading activations detected: " + cascadingActivations.get());
        if (cascadingActivations.get() > 0) {
            System.out.println("RESULT: Cascading triggers WORKED!");
        } else {
            System.err.println("RESULT: Cascading triggers FAILED to detect materialized relationship.");
        }
    }

    private Map<String, QueryResult> runWithTrigger(String modeLabel) throws IOException {
        clearDatabase();
        TriggerRegistryInterface.PathActivation activation = new TriggerRegistryInterface.PathActivation(
                PATTERN, 0, TriggerRegistryInterface.EventType.ON_CREATE);

        AtomicLong lastTriggerDetectedTime = new AtomicLong(0);
        AtomicLong triggerCount = new AtomicLong(0);

        String triggerId = orchestrator.register(new FullTrigger(
                modeLabel.toLowerCase() + "-experiment-trigger",
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

        Map<String, QueryResult> results = runFullSequence(true, lastTriggerDetectedTime, triggerCount, false,
                modeLabel);
        orchestrator.unregister(triggerId);
        return results;
    }

    private Map<String, QueryResult> runWithCascadingTrigger(String modeLabel) throws IOException {
        clearDatabase();
        TriggerRegistryInterface.PathActivation activation = new TriggerRegistryInterface.PathActivation(
                PATTERN, 0, TriggerRegistryInterface.EventType.ON_CREATE);

        AtomicLong lastTriggerDetectedTime = new AtomicLong(0);
        AtomicLong triggerCount = new AtomicLong(0);

        String triggerId = orchestrator.register(new FullTrigger(
                modeLabel.toLowerCase() + "-cascading-trigger",
                TriggerRegistryInterface.Scope.PATH,
                activation,
                100,
                true,
                ctx -> true,
                committed -> {
                    lastTriggerDetectedTime.set(System.nanoTime());
                    triggerCount.incrementAndGet();

                    for (TriggerRegistryInterface.PathMatch match : committed.matches()) {
                        String startNodeId = match.nodeIds().get(0);
                        String endNodeId = match.nodeIds().get(match.nodeIds().size() - 1);

                        java.util.concurrent.CompletableFuture.runAsync(() -> {
                            try (Transaction tx = committed.db().beginTx()) {
                                Node start = tx.getNodeByElementId(startNodeId);
                                Node end = tx.getNodeByElementId(endNodeId);

                                // Existence check
                                boolean exists = false;
                                for (Relationship r : start.getRelationships(Direction.OUTGOING,
                                        RelationshipType.withName("guarantee"))) {
                                    if (r.getEndNode().getElementId().equals(endNodeId)) {
                                        exists = true;
                                        break;
                                    }
                                }

                                if (!exists) {
                                    start.createRelationshipTo(end, RelationshipType.withName("guarantee"));
                                    tx.commit();
                                } else {
                                    tx.rollback();
                                }
                            } catch (Exception e) {
                                // Ignore errors in async materialization
                            }
                        });
                    }
                },
                TriggerRegistryInterface.Time.AFTER_COMMIT,
                0));

        Map<String, QueryResult> results = runFullSequence(true, lastTriggerDetectedTime, triggerCount, false,
                modeLabel);
        orchestrator.unregister(triggerId);
        return results;
    }

    private Map<String, QueryResult> runFullSequence(boolean withTrigger, AtomicLong lastTriggerDetectedTime,
            AtomicLong triggerCount,
            boolean isWarmup) throws IOException {
        return runFullSequence(withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup,
                withTrigger ? "WITH" : "BASELINE");
    }

    private Map<String, QueryResult> runFullSequence(boolean withTrigger, AtomicLong lastTriggerDetectedTime,
            AtomicLong triggerCount,
            boolean isWarmup, String modeLabel) throws IOException {
        Map<String, QueryResult> results = new HashMap<>();

        QueryResult r1 = measureQuery("tw-1.cypher", "snapshot/Person.csv",
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "currentTime" },
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);
        if (r1 != null)
            results.put("tw-1.cypher", r1);

        QueryResult r10 = measureQuery("tw-10.cypher", "incremental/AddPersonGuaranteePersonWrite10.csv",
                new String[] { "pid1", "pid2", "currentTime" },
                new String[] { "fromId", "toId", "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);
        if (r10 != null)
            results.put("tw-10.cypher", r10);

        return results;
    }

    private static Map<String, Double> baselines = new HashMap<>();

    private QueryResult measureQuery(String queryFile, String csvFile, String[] paramKeys, String[] csvHeaders,
            boolean withTrigger, AtomicLong lastTriggerDetectedTime, AtomicLong triggerCount,
            boolean isWarmup, String modeLabel) throws IOException {
        String query = loadQuery(queryFile);
        List<Map<String, Object>> dataset = loadCsv(csvFile, paramKeys, csvHeaders);

        List<Long> txTimes = new ArrayList<>();
        List<Long> activationLatencies = new ArrayList<>();
        List<Double> memorySamples = new ArrayList<>();

        // Specialized lists for only-firing transactions
        List<Long> firingTxTimes = new ArrayList<>();
        List<Double> firingMemorySamples = new ArrayList<>();

        long startTriggerCount = triggerCount != null ? triggerCount.get() : 0;

        System.gc();

        for (Map<String, Object> params : dataset) {
            long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long startTime = System.nanoTime();
            long commitStartTime;
            try (Transaction tx = db.beginTx()) {
                tx.execute(query, params);
                commitStartTime = System.nanoTime();
                tx.commit();
            }
            long endTime = System.nanoTime();
            long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            long duration = endTime - startTime;
            double memDelta = (double) (memAfter - memBefore) / (1024.0 * 1024.0);

            txTimes.add(duration);
            memorySamples.add(memDelta);

            if (withTrigger && lastTriggerDetectedTime != null && lastTriggerDetectedTime.get() > commitStartTime) {
                // This transaction fired a trigger
                firingTxTimes.add(duration);
                firingMemorySamples.add(memDelta);

                long latency = lastTriggerDetectedTime.get() - commitStartTime;
                activationLatencies.add(Math.max(0L, latency));
            }
        }

        if (isWarmup)
            return null;

        int totalCount = dataset.size();
        long totalActivations = (triggerCount != null) ? triggerCount.get() - startTriggerCount : 0;

        // Determine which dataset to use for averages (Averages of Firing-Only)
        List<Long> targetTxTimes = (withTrigger && !firingTxTimes.isEmpty()) ? firingTxTimes : txTimes;
        List<Double> targetMemory = (withTrigger && !firingMemorySamples.isEmpty()) ? firingMemorySamples
                : memorySamples;

        int count = targetTxTimes.size();
        double avgTxMs = (targetTxTimes.stream().mapToLong(Long::longValue).sum() / (double) count) / 1_000_000.0;
        double stdDevTx = Math.sqrt(
                targetTxTimes.stream().mapToDouble(t -> Math.pow((t / 1_000_000.0) - avgTxMs, 2)).average()
                        .orElse(0.0));

        double avgActMs = 0;
        double stdDevAct = 0;
        if (!activationLatencies.isEmpty()) {
            avgActMs = (activationLatencies.stream().mapToLong(Long::longValue).sum()
                    / (double) activationLatencies.size()) / 1_000_000.0;
            double finalAvgActMs = avgActMs;
            stdDevAct = Math.sqrt(activationLatencies.stream()
                    .mapToDouble(l -> Math.pow((l / 1_000_000.0) - finalAvgActMs, 2)).average().orElse(0.0));
        }

        double avgMem = targetMemory.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double finalAvgMem = Math.max(0, avgMem);
        double stdDevMem = Math.sqrt(targetMemory.stream().mapToDouble(m -> Math.pow(Math.max(0, m) - finalAvgMem, 2))
                .average().orElse(0.0));

        double overhead = 0;
        if (modeLabel.equals("BASELINE")) {
            baselines.put(queryFile, avgTxMs);
        } else {
            Double baseline = baselines.get(queryFile);
            if (baseline != null && baseline > 0)
                overhead = ((avgTxMs - baseline) / baseline) * 100.0;
        }

        System.out.printf("%-13s | %-8s | %5d | %16.4f | %11.4f | %20.4f | %14.4f | %11d | %12.2f | %12.3f | %10.4f\n",
                queryFile, modeLabel, totalCount, avgTxMs, stdDevTx, avgActMs, stdDevAct, totalActivations,
                Math.max(0.0, overhead), finalAvgMem, stdDevMem);

        return new QueryResult(avgTxMs, stdDevTx, avgActMs, stdDevAct, finalAvgMem, stdDevMem, overhead);
    }
}
