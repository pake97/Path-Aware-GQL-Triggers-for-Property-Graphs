package org.lyon1.trigger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lyon1.Listener;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.logging.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Experiment1Test {

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

    private static final String INCREMENTAL_PATH = "src/test/resources/sf0.01/incremental/";
    private static final String QUERIES_PATH = "src/test/resources/queries/";
    private static final String PATTERN = "(:Person)-[:own]->(:Account)<-[:deposit]-(:Loan)";

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
        try (BufferedReader br = new BufferedReader(new FileReader(INCREMENTAL_PATH + filename))) {
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
    void runExperiment1() throws IOException {
        System.out.println("Experiment 1: Performance Comparison (Baseline vs INDEX vs PATH)");
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

        // Summary Line for tw-15.cypher
        QueryResult idxRes = indexResults.get("tw-15.cypher");
        QueryResult pathRes = pathResults.get("tw-15.cypher");

        if (idxRes != null && pathRes != null) {
            System.out.println();
            System.out.printf("Summary (tw-15): %.4f (%.4f) %.4f (%.4f) %.2f %.2f %.4f (%.4f) %.4f (%.4f)\n",
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

        QueryResult r1 = measureQuery("tw-1.cypher", "AddPersonWrite1.csv",
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "currentTime" },
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);
        if (r1 != null)
            results.put("tw-1.cypher", r1);

        QueryResult r4 = measureQuery("tw-4.cypher", "AddPersonOwnAccountWrite4.csv",
                new String[] { "personId", "accountId", "accountType", "accountBlocked", "currentTime" },
                new String[] { "personId", "accountId", "accountType", "accountBlocked", "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);
        if (r4 != null)
            results.put("tw-4.cypher", r4);

        QueryResult r6 = measureQuery("tw-6.cypher", "AddPersonApplyLoanWrite6.csv",
                new String[] { "personId", "loanId", "amount", "currentTime" },
                new String[] { "personId", "loanId", "loanAmount", "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);
        if (r6 != null)
            results.put("tw-6.cypher", r6);

        QueryResult r15 = measureQuery("tw-15.cypher", "AddLoanDepositAccountWrite15.csv",
                new String[] { "loanId", "accountId", "amount", "currentTime" },
                new String[] { "loanId", "accountId", "amount", "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);
        if (r15 != null)
            results.put("tw-15.cypher", r15);

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

                // Delay since the commit started
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
            if (baseline != null && baseline > 0) {
                overhead = ((avgTxMs - baseline) / baseline) * 100.0;
            }
        }

        System.out.printf("%-13s | %-8s | %5d | %16.4f | %11.4f | %20.4f | %14.4f | %11d | %12.2f | %12.3f | %10.4f\n",
                queryFile, modeLabel, totalCount, avgTxMs, stdDevTx, avgActMs, stdDevAct, totalActivations,
                Math.max(0.0, overhead), finalAvgMem, stdDevMem);

        return new QueryResult(avgTxMs, stdDevTx, avgActMs, stdDevAct, finalAvgMem, stdDevMem, overhead);
    }
}
