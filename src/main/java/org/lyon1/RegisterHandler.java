package org.lyon1;


import org.lyon1.trigger.*;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Paths;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Singleton
@Path("/triggers")
@Produces(MediaType.APPLICATION_JSON)
public final class RegisterHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RegisterHandler.class);

    private final DatabaseManagementService dbms;
    private final Log neoLog;

    // Core singletons used by your plugin
    private TriggerRegistry registry;
    private InMemoryOrchestrator orchestrator;

    // Listener lifecycle
    private volatile TransactionEventListener<?> listener;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private volatile String registeredDb = null;
    private TriggerRegistryFactory trFactory;

    public RegisterHandler(@Context DatabaseManagementService dbms, @Context Log log) {
        this.dbms = dbms;
        this.neoLog = log;
        this .trFactory = new TriggerRegistryFactory();

    }

    /** Register the transaction listener (idempotent). */
    @POST @Path("/register")
    public Response register(@QueryParam("db") @DefaultValue(DEFAULT_DATABASE_NAME) String dbName,
                             @QueryParam("type") @DefaultValue("AUTOMATON") String type) {
        try {
            if (registered.get()) {
                if (dbName.equals(registeredDb)) {
                    return Response.ok(json(Map.of("status","already_registered","db",dbName))).build();
                }
                return Response.status(Response.Status.CONFLICT)
                        .entity(json(Map.of("error","listener already registered on db " + registeredDb))).build();
            }

            if(type.equals("AUTOMATON")) {
                // 1) Create the registry & orchestrator
                this.registry = trFactory.create(TriggerType.AUTOMATON, dbms);

            } else {
                // Fallback to basic registry/orchestrator
                this.registry = trFactory.create(TriggerType.INDEX, dbms);

            }

            this.orchestrator = new InMemoryOrchestrator(registry);
            // (optional) observe registry version bumps

            registry.addListener(snap -> neoLog.info("Trigger registry updated: v{}", snap.version()));

            // 2) Create the listener, pass orchestrator/registry
            // If you have your own Listener class, make sure its ctor accepts these.
            TransactionEventListener<?> l = new Listener(neoLog, orchestrator, registry);

            dbms.registerTransactionEventListener(dbName, l);

            listener = l;
            registeredDb = dbName;
            registered.set(true);


            // inside your /register endpoint, after successful registration:
            try {
                TriggerInstaller installer = new TriggerInstaller(orchestrator, neoLog);
                // load from a resource on the classpath, or a file path:
                // try (InputStream in = getClass().getResourceAsStream("/triggers.yaml")) { ... }
                installer.installFromYaml(Paths.get("/Users/amedeo/Downloads/neo4j-community-2025.05.0/conf/triggers.yaml"));
            } catch (Exception ex) {
                neoLog.warn(ex.getMessage());
                neoLog.warn("Failed to install YAML triggers: {}", ex.getMessage());
            }

            neoLog.info("Registered listener on db '{}'", dbName);
            LOG.info("Registered listener on db '{}'", dbName);

            return Response.ok(json(Map.of("status","registered","db",dbName))).build();
        } catch (Exception e) {
            LOG.error("Failed to register listener", e);
            neoLog.error("Failed to register listener: " + e.getMessage(), e);
            return Response.serverError().entity(json(Map.of(
                    "error", e.getClass().getSimpleName() + ": " + e.getMessage()))).build();
        }
    }

    /** Unregister (idempotent). */
    @DELETE @Path("/register")
    public Response unregister() {
        if (!registered.get()) return Response.ok(json(Map.of("status","not_registered"))).build();
        try {
            dbms.unregisterTransactionEventListener(registeredDb, listener);
            String db = registeredDb;
            registered.set(false);
            listener = null;
            registeredDb = null;
            neoLog.info("Unregistered listener from db '{}'", db);
            return Response.ok(json(Map.of("status","unregistered","db",db))).build();
        } catch (Exception e) {
            LOG.error("Failed to unregister listener", e);
            neoLog.error("Failed to unregister listener: " + e.getMessage(), e);
            return Response.serverError().entity(json(Map.of(
                    "error", e.getClass().getSimpleName() + ": " + e.getMessage()))).build();
        }
    }

    /** Status. */
    @GET @Path("/status")
    public Response status() {
        return registered.get()
                ? Response.ok(json(Map.of("registered",true,"db",registeredDb))).build()
                : Response.ok(json(Map.of("registered",false))).build();
    }

    /* ---------------- Optional: quick demo endpoint to add a trigger ---------------- */

    // Example payload (very simple):
    // { "label": "Person", "event": "ON_CREATE", "priority": 10 }
    @POST @Path("/add")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response add(Map<String,Object> body) {
        String label = String.valueOf(body.getOrDefault("label", "Person"));
        String event = String.valueOf(body.getOrDefault("event", "ON_CREATE"));
        int priority = ((Number) body.getOrDefault("priority", 100)).intValue();

        // Build activation
        TriggerRegistryInterface.NodeActivation activation = new TriggerRegistryInterface.NodeActivation(
                Set.of(label), Map.of(), TriggerRegistryInterface.EventType.valueOf(event));

        // Register a minimal FullTrigger (predicate always true, action logs)
        String id = orchestrator.register(
                new FullTrigger(
                        null, // id -> generated
                        TriggerRegistryInterface.Scope.NODE,
                        activation,
                        priority,
                        true,
                        ctx -> true,
                        committed -> neoLog.info("Trigger '{}' fired for label={}", "node-"+label, label),
                        TriggerRegistryInterface.Time.AFTER_COMMIT,
                        0
                )
        );

        return Response.ok(json(Map.of("status","added","id",id))).build();
    }

    /* ---------------- Utils ---------------- */

    private static String json(Map<?,?> m) {
        // tiny JSON builder; replace with ObjectMapper if you prefer
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var e : m.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(escape(String.valueOf(e.getKey()))).append('"').append(':');
            Object v = e.getValue();
            if (v instanceof Number || v instanceof Boolean) {
                sb.append(String.valueOf(v));
            } else {
                sb.append('"').append(escape(String.valueOf(v))).append('"');
            }
        }
        return sb.append('}').toString();
    }
    private static String escape(String s) { return s.replace("\"","\\\""); }
}
