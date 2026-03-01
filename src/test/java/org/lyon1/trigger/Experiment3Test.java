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
public class Experiment3Test {

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
    private static final int[] TRIGGER_COUNTS = { 1, 2, 10, 20, 30, 50 };// { 1, 3, 5, 10 };

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
    void runExperiment3() throws IOException {
        System.out.println("Experiment 3: Fan-out Scaling — Latency vs Number of Triggers (N = 1, 3, 5, 10)");
        System.out.println("Trigger Pattern: " + PATTERN);
        System.out.println();
        System.out.println(
                "Query         | Mode     | Count | Avg Tx Time (ms) | StdDev Time | Avg Act Latency (ms) | StdDev Latency | Activations | Overhead (%) | Avg Mem (MB) | StdDev Mem");
        System.out.println(
                "--------------|----------|-------|------------------|-------------|----------------------|----------------|-------------|--------------|--------------|-----------");

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
        runFullSequence(false, null, null, false);

        // 3. INDEX with N = 1, 3, 5, 10
        System.err.println("--- Starting Index Phase ---");
        switchRegistry(TriggerType.INDEX);
        for (int n : TRIGGER_COUNTS) {
            System.err.println("  INDEX N=" + n);
            runWithTrigger("INDEX-" + n, n);
        }

        // 4. PATH (Automaton) with N = 1, 3, 5, 10
        System.err.println("--- Starting Path Phase ---");
        switchRegistry(TriggerType.AUTOMATON);
        for (int n : TRIGGER_COUNTS) {
            System.err.println("  PATH N=" + n);
            runWithTrigger("PATH-" + n, n);
        }
    }

    private void runWithTrigger(String modeLabel, int n) throws IOException {
        clearDatabase();
        TriggerRegistryInterface.PathActivation activation = new TriggerRegistryInterface.PathActivation(
                PATTERN, 0, TriggerRegistryInterface.EventType.ON_CREATE);

        AtomicLong lastTriggerDetectedTime = new AtomicLong(0);
        AtomicLong triggerCount = new AtomicLong(0);
        System.gc();
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        List<String> triggerIds = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            String triggerId = orchestrator.register(new FullTrigger(
                    modeLabel.toLowerCase() + "-trigger-" + i,
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
            triggerIds.add(triggerId);
        }
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.printf("[%s] Heap delta after trigger registration: %.3f MB%n",
                modeLabel, (memAfter - memBefore) / (1024.0 * 1024.0));

        runFullSequence(true, lastTriggerDetectedTime, triggerCount, false, modeLabel);

        for (String id : triggerIds) {
            orchestrator.unregister(id);
        }
    }

    private void runFullSequence(boolean withTrigger, AtomicLong lastTriggerDetectedTime, AtomicLong triggerCount,
            boolean isWarmup) throws IOException {
        runFullSequence(withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup,
                withTrigger ? "WITH" : "BASELINE");
    }

    private void runFullSequence(boolean withTrigger, AtomicLong lastTriggerDetectedTime, AtomicLong triggerCount,
            boolean isWarmup, String modeLabel) throws IOException {
        measureQuery("tw-1.cypher", "AddPersonWrite1.csv",
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "currentTime" },
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);

        measureQuery("tw-4.cypher", "AddPersonOwnAccountWrite4.csv",
                new String[] { "personId", "accountId", "accountType", "accountBlocked", "currentTime" },
                new String[] { "personId", "accountId", "accountType", "accountBlocked", "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);

        measureQuery("tw-6.cypher", "AddPersonApplyLoanWrite6.csv",
                new String[] { "personId", "loanId", "amount", "currentTime" },
                new String[] { "personId", "loanId", "loanAmount", "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);

        measureQuery("tw-15.cypher", "AddLoanDepositAccountWrite15.csv",
                new String[] { "loanId", "accountId", "amount", "currentTime" },
                new String[] { "loanId", "accountId", "amount", "createTime" },
                withTrigger, lastTriggerDetectedTime, triggerCount, isWarmup, modeLabel);
    }

    private static Map<String, Double> baselines = new HashMap<>();

    private void measureQuery(String queryFile, String csvFile, String[] paramKeys, String[] csvHeaders,
            boolean withTrigger, AtomicLong lastTriggerDetectedTime, AtomicLong triggerCount,
            boolean isWarmup, String modeLabel) throws IOException {
        String query = loadQuery(queryFile);
        List<Map<String, Object>> dataset = loadCsv(csvFile, paramKeys, csvHeaders);

        List<Long> txTimes = new ArrayList<>();
        List<Long> activationLatencies = new ArrayList<>();
        List<Double> memorySamples = new ArrayList<>();
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

            txTimes.add(endTime - startTime);
            memorySamples.add((double) (memAfter - memBefore) / (1024.0 * 1024.0));

            if (withTrigger && lastTriggerDetectedTime != null && lastTriggerDetectedTime.get() > commitStartTime) {
                // Delay since the commit started, captured when the last trigger fires
                long latency = lastTriggerDetectedTime.get() - commitStartTime;
                activationLatencies.add(Math.max(0L, latency));
            }
        }

        if (isWarmup)
            return;

        int count = dataset.size();
        double avgTxMs = (txTimes.stream().mapToLong(Long::longValue).sum() / (double) count) / 1_000_000.0;
        double stdDevTx = Math.sqrt(
                txTimes.stream().mapToDouble(t -> Math.pow((t / 1_000_000.0) - avgTxMs, 2)).average().orElse(0.0));

        double avgActMs = 0;
        double stdDevAct = 0;
        if (!activationLatencies.isEmpty()) {
            avgActMs = (activationLatencies.stream().mapToLong(Long::longValue).sum()
                    / (double) activationLatencies.size()) / 1_000_000.0;
            double finalAvgActMs = avgActMs;
            stdDevAct = Math.sqrt(activationLatencies.stream()
                    .mapToDouble(l -> Math.pow((l / 1_000_000.0) - finalAvgActMs, 2)).average().orElse(0.0));
        }

        double avgMem = memorySamples.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double finalAvgMem = Math.max(0, avgMem);
        double stdDevMem = Math.sqrt(memorySamples.stream().mapToDouble(m -> Math.pow(Math.max(0, m) - finalAvgMem, 2))
                .average().orElse(0.0));

        long totalActivations = (triggerCount != null) ? triggerCount.get() - startTriggerCount : 0;

        double overhead = 0;
        if (modeLabel.equals("BASELINE")) {
            baselines.put(queryFile, avgTxMs);
        } else {
            Double baseline = baselines.get(queryFile);
            if (baseline != null && baseline > 0) {
                overhead = ((avgTxMs - baseline) / baseline) * 100.0;
            }
        }

        System.out.printf("%-13s | %-8s | %5d | %16.4f | %11.4f | %20.2f | %14.4f | %11d | %12.2f | %12.3f | %10.4f\n",
                queryFile, modeLabel, count, avgTxMs, stdDevTx, avgActMs, stdDevAct, totalActivations,
                Math.max(0.0, overhead), finalAvgMem, stdDevMem);
    }
}
