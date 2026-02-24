package org.lyon1.trigger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FinBenchIncrementalPerformanceTest {

    private Neo4j neo4j;
    private GraphDatabaseService db;

    private static final String INCREMENTAL_PATH = "src/test/resources/sf0.01/incremental/";
    private static final String QUERIES_PATH = "src/test/resources/queries/";

    @BeforeAll
    void setup() {
        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .build();
        db = neo4j.defaultDatabaseService();
    }

    @AfterAll
    void tearDown() {
        if (neo4j != null) {
            neo4j.close();
        }
    }

    private String loadQuery(String filename) throws IOException {
        return new String(Files.readAllBytes(Paths.get(QUERIES_PATH + filename))).trim();
    }

    private List<Map<String, Object>> loadCsv(String filename, String[] expectedKeys, String[] csvHeaders)
            throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(INCREMENTAL_PATH + filename))) {
            String line = br.readLine(); // Skip header
            if (line == null)
                return data;

            // Map header indices
            String[] actualHeaders = line.split("\\|");
            Map<String, Integer> headerIndex = new HashMap<>();
            for (int i = 0; i < actualHeaders.length; i++) {
                headerIndex.put(actualHeaders[i], i);
            }

            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|");
                Map<String, Object> params = new HashMap<>();
                for (int i = 0; i < expectedKeys.length; i++) {
                    String csvKey = csvHeaders[i];
                    String paramKey = expectedKeys[i];
                    if (headerIndex.containsKey(csvKey)) {
                        String val = values[headerIndex.get(csvKey)];
                        if (val.equals("true") || val.equals("false")) {
                            params.put(paramKey, Boolean.parseBoolean(val));
                        } else {
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
    void runIncrementalPerformanceTest() throws IOException {
        System.out.println(
                "Query         | Count | Avg Tx Time (ms) | StdDev Time | Total Time (ms) | Avg Mem (MB) | StdDev Mem");
        System.out.println(
                "--------------|-------|------------------|-------------|-----------------|--------------|-----------");

        // TW 1
        measureQuery("tw-1.cypher", "AddPersonWrite1.csv",
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "currentTime" },
                new String[] { "personId", "personName", "isBlocked", "gender", "birthday", "country", "city",
                        "createTime" });

        // TW 4
        measureQuery("tw-4.cypher", "AddPersonOwnAccountWrite4.csv",
                new String[] { "personId", "accountId", "accountType", "accountBlocked", "currentTime" },
                new String[] { "personId", "accountId", "accountType", "accountBlocked", "createTime" });

        // TW 6
        measureQuery("tw-6.cypher", "AddPersonApplyLoanWrite6.csv",
                new String[] { "personId", "loanId", "amount", "currentTime" },
                new String[] { "personId", "loanId", "loanAmount", "createTime" });

        // TW 15
        measureQuery("tw-15.cypher", "AddLoanDepositAccountWrite15.csv",
                new String[] { "loanId", "accountId", "amount", "currentTime" },
                new String[] { "loanId", "accountId", "amount", "createTime" });
    }

    private void measureQuery(String queryFile, String csvFile, String[] paramKeys, String[] csvHeaders)
            throws IOException {
        String query = loadQuery(queryFile);
        List<Map<String, Object>> dataset = loadCsv(csvFile, paramKeys, csvHeaders);

        List<Long> times = new ArrayList<>();
        List<Double> memorySamples = new ArrayList<>();
        int count = dataset.size();

        System.gc();

        for (Map<String, Object> params : dataset) {
            long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long startTime = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                tx.execute(query, params);
                tx.commit();
            }
            long endTime = System.nanoTime();
            long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            times.add(endTime - startTime);
            memorySamples.add((double) (memAfter - memBefore) / (1024.0 * 1024.0));
        }

        double totalTimeMs = times.stream().mapToLong(Long::longValue).sum() / 1_000_000.0;
        double avgTimeMs = totalTimeMs / count;

        double varianceTime = times.stream()
                .mapToDouble(t -> Math.pow((t / 1_000_000.0) - avgTimeMs, 2))
                .average().orElse(0.0);
        double stdDevTime = Math.sqrt(varianceTime);

        double avgMemMB = memorySamples.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        if (avgMemMB < 0)
            avgMemMB = 0;

        double finalAvgMemMB = avgMemMB;
        double varianceMem = memorySamples.stream()
                .mapToDouble(m -> Math.pow(Math.max(0, m) - finalAvgMemMB, 2))
                .average().orElse(0.0);
        double stdDevMem = Math.sqrt(varianceMem);

        System.out.printf("%-13s | %5d | %16.4f | %11.4f | %15.2f | %12.2f | %10.4f\n",
                queryFile, count, avgTimeMs, stdDevTime, totalTimeMs, finalAvgMemMB, stdDevMem);
    }
}
