package org.lyon1;

import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExtensionTest {

    private Neo4j neo4j;
    private final ByteArrayOutputStream logCapture = new ByteArrayOutputStream();
    private PrintStream originalOut;

    @BeforeAll
    void setup() {
        originalOut = System.out;
        System.setOut(new PrintStream(logCapture));

        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withUnmanagedExtension("/path", RegisterHandler.class)
                .build();
    }

    @AfterAll
    void tearDown() {
        if (neo4j != null) {
            neo4j.close();
        }
        System.setOut(originalOut);
    }

    @Test
    @Order(1)
    void testExtensionStatus() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(neo4j.httpURI().toString() + "path/triggers/status"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"registered\":false"));
    }

    @Test
    @Order(2)
    void testRegisterExtension() throws Exception {
        HttpClient client = HttpClient.newHttpClient();

        // Unregister first to ensure clean state
        HttpRequest unregisterRequest = HttpRequest.newBuilder()
                .uri(URI.create(neo4j.httpURI().toString() + "path/triggers/register"))
                .DELETE()
                .build();
        client.send(unregisterRequest, HttpResponse.BodyHandlers.ofString());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(neo4j.httpURI().toString() + "path/triggers/register"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"registered\""));

        // Verify status again
        HttpRequest statusRequest = HttpRequest.newBuilder()
                .uri(URI.create(neo4j.httpURI().toString() + "path/triggers/status"))
                .GET()
                .build();

        HttpResponse<String> statusResponse = client.send(statusRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, statusResponse.statusCode());
        assertTrue(statusResponse.body().contains("\"registered\":true"));
    }

    @Test
    @Order(3)
    void testRegisterWithYaml() throws Exception {
        // Find triggers.yaml in src/test/resources
        java.nio.file.Path yamlPath = java.nio.file.Paths.get("src", "test", "resources", "triggers.yaml")
                .toAbsolutePath();
        String encodedPath = java.net.URLEncoder.encode(yamlPath.toString(), java.nio.charset.StandardCharsets.UTF_8);

        HttpClient client = HttpClient.newHttpClient();

        // Unregister first to ensure clean state
        HttpRequest unregisterRequest = HttpRequest.newBuilder()
                .uri(URI.create(neo4j.httpURI().toString() + "path/triggers/register"))
                .DELETE()
                .build();
        client.send(unregisterRequest, HttpResponse.BodyHandlers.ofString());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(neo4j.httpURI().toString() + "path/triggers/register?config=" + encodedPath))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"registered\""));
    }

    @Test
    @Order(4)
    void testTriggerExecution() throws Exception {
        // 1) Ensure YAML triggers are registered (from testRegisterWithYaml)

        // 2) Create a node with label Person
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), AuthTokens.none());
                Session session = driver.session()) {
            session.run("CREATE (:Person {name: 'Test'})").consume();
        }

        // 3) Check if the log of neo4j contains the message
        // Captured triggers with type: stdout go to System.out
        String captured = logCapture.toString();
        assertTrue(captured.contains("A node Person has been created and seen by the trigger person-create"),
                "Log should contain verification message. Captured: " + captured);
    }
}
