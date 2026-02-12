package org.lyon1.trigger;

import org.neo4j.logging.Log;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class TriggerInstaller {

    private final InMemoryOrchestrator orchestrator;
    private final Log log;

    public TriggerInstaller(InMemoryOrchestrator orchestrator, Log log) {
        this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator");
        this.log = Objects.requireNonNull(log, "log");
    }

    /** Convenience: install from a file path. */
    public List<String> installFromYaml(Path path) throws Exception {
        log.info("Path for trigger config YAML " + path.toString());
        try (InputStream in = Files.newInputStream(path)) {
            return installFromYaml(in);
        }
    }

    /**
     * Install from a YAML InputStream. Returns the list of trigger IDs
     * created/replaced.
     */
    @SuppressWarnings("unchecked")
    public List<String> installFromYaml(InputStream yamlStream) {
        Objects.requireNonNull(yamlStream, "yamlStream");
        Yaml yaml = new Yaml();
        Object loaded = yaml.load(yamlStream);
        log.warn("YAML LOADED " + loaded.toString());
        if (!(loaded instanceof Map<?, ?> root)) {
            throw new IllegalArgumentException("YAML root must be a mapping with key 'triggers'");
        }
        Object triggersNode = root.get("triggers");
        if (!(triggersNode instanceof List<?> list)) {
            throw new IllegalArgumentException("'triggers' must be a sequence");
        }

        List<String> ids = new ArrayList<>();
        for (Object o : list) {
            if (!(o instanceof Map<?, ?> m))
                continue;

            String id = str(m.get("id")); // optional
            String scopeStr = reqStr(m, "scope"); // NODE | RELATIONSHIP | PATH
            String eventStr = reqStr(m, "event"); // ON_CREATE | ON_DELETE
            Integer priority = intOr(m.get("priority"), 100);
            Boolean enabled = boolOr(m.get("enabled"), true);
            String timeStr = str(m.get("time"));
            TriggerRegistryInterface.Time time = (timeStr == null)
                    ? TriggerRegistryInterface.Time.AFTER_COMMIT
                    : TriggerRegistryInterface.Time.valueOf(timeStr);
            int order = ids.size();

            TriggerRegistryInterface.Scope scope = TriggerRegistryInterface.Scope.valueOf(scopeStr);
            TriggerRegistryInterface.EventType event = TriggerRegistryInterface.EventType.valueOf(eventStr);

            // Activation (one of: labels, relType, pathSignature)
            TriggerRegistryInterface.Activation activation = switch (scope) {
                case NODE -> {
                    Set<String> labels = setOfStrings(m.get("labels"));
                    if (labels.isEmpty()) {
                        throw new IllegalArgumentException("NODE trigger requires non-empty 'labels'");
                    }
                    yield new TriggerRegistryInterface.NodeActivation(labels, Map.of(), event);
                }
                case RELATIONSHIP -> {
                    String relType = reqStr(m, "types");
                    // startLabels/endLabels are optional
                    Set<String> start = setOfStrings(m.get("startLabels"));
                    Set<String> end = setOfStrings(m.get("endLabels"));
                    yield new TriggerRegistryInterface.RelActivation(relType, start, end, event);
                }
                case PATH -> {
                    String signature = reqStr(m, "pathSignature");
                    int maxLen = intOr(m.get("maxLen"), 4);
                    yield new TriggerRegistryInterface.PathActivation(signature, maxLen, event);
                }
            };

            // Action (minimal built-ins)
            Consumer<CommittedContext> action = buildAction(m.get("action"));

            // Predicate (default: always true; extend to read property filters if you like)
            Predicate<TxContext> predicate = ctx -> true;

            // Register (generate an id if absent)
            var full = new FullTrigger(
                    id, scope, activation, priority, enabled, predicate, action, time, order);

            String assignedId = (id == null || id.isBlank())
                    ? orchestrator.register(full)
                    : (orchestrator.replace(full) ? id : orchestrator.register(full)); // replace if exists, else add

            log.info("Installed trigger id=" + assignedId +
                    " scope=" + scope +
                    " event=" + event +
                    " enabled=" + enabled);

            ids.add(assignedId);
        }

        return ids;
    }

    /* ---------------- helpers ---------------- */

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o).trim();
    }

    private static String reqStr(Map<?, ?> m, String key) {
        Object v = m.get(key);
        if (v == null)
            throw new IllegalArgumentException("Missing required key: " + key);
        String s = String.valueOf(v).trim();
        if (s.isEmpty())
            throw new IllegalArgumentException("Empty value for key: " + key);
        return s;
    }

    private static int intOr(Object o, int def) {
        if (o instanceof Number n)
            return n.intValue();
        if (o == null)
            return def;
        return Integer.parseInt(String.valueOf(o));
    }

    private static boolean boolOr(Object o, boolean def) {
        if (o instanceof Boolean b)
            return b;
        if (o == null)
            return def;
        return Boolean.parseBoolean(String.valueOf(o));
    }

    @SuppressWarnings("unchecked")
    private static Set<String> setOfStrings(Object o) {
        if (o == null)
            return Set.of();
        if (o instanceof String s)
            return Set.of(s);
        if (o instanceof Collection<?> c) {
            LinkedHashSet<String> out = new LinkedHashSet<>();
            for (Object e : c)
                if (e != null)
                    out.add(String.valueOf(e).trim());
            return Collections.unmodifiableSet(out);
        }
        throw new IllegalArgumentException("Expected string or list of strings, got: " + o.getClass());
    }

    @SuppressWarnings("unchecked")
    private Consumer<CommittedContext> buildAction(Object node) {
        if (!(node instanceof Map<?, ?> m)) {
            return ctx -> log.info("Trigger fired (no action configured)");
        }

        final String normalizedType = Optional.ofNullable(str(m.get("type"))).orElse("log");

        switch (normalizedType) {
            case "log" -> {
                final String msg = Optional.ofNullable(str(m.get("message"))).orElse("Trigger fired");
                final String lvl = Optional.ofNullable(str(m.get("level"))).orElse("info").toLowerCase(Locale.ROOT);
                return ctx -> {
                    switch (lvl) {
                        case "warn" -> log.warn(msg);
                        case "error" -> log.error(msg);
                        case "debug" -> log.debug(msg);
                        default -> log.info(msg);
                    }
                };
            }
            case "stdout" -> {
                final String msg = Optional.ofNullable(str(m.get("message"))).orElse("Trigger fired");
                return ctx -> System.out.println(msg);
            }
            case "noop" -> {
                return ctx -> {
                };
            }
            default -> {
                final String typeForMsg = normalizedType; // capture a final
                return ctx -> log.info("Trigger fired (unknown action '{}')", typeForMsg);
            }
        }
    }
}
