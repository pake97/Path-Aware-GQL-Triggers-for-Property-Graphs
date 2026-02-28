# GQLTrigger

A trigger system for Neo4j that fires reactive rules when graph transactions match a declared path pattern. Two detection strategies are implemented and benchmarked against each other: an **index-based** approach and an **automaton-based** approach.

## Overview

A trigger is a rule composed of:
- a **pattern** — a Cypher-style path expression such as `(:Person)-[:own]->(:Account)<-[:deposit]-(:Loan)`
- an **event type** — `ON_CREATE` or `ON_DELETE`
- a **time** — `BEFORE_COMMIT` or `AFTER_COMMIT`
- a **predicate** — an optional condition evaluated on the transaction context
- an **action** — a callback invoked when the trigger fires

After every committed transaction, the system inspects the graph changes and fires any trigger whose pattern is now satisfied.

## Architecture

```
Neo4j Transaction
       │
       ▼
   Listener                       (TransactionEventListener)
       │
       ▼
 InMemoryOrchestrator             (owns FullTrigger objects with predicate + action)
       │
       ├── TriggerRegistry        (inverted indexes: label/rel-type → trigger IDs)
       │
       └── PathMonitor
             ├── IndexPathMonitor      (INDEX strategy)
             └── AutomatonPathMonitor  (AUTOMATON strategy)
```

### Detection strategies

| Strategy | Mechanism |
|---|---|
| **INDEX** | Inverted index keyed by label and relationship type. Candidate triggers are looked up per graph element changed in the transaction, then verified against the full pattern. |
| **AUTOMATON** | Each pattern is compiled into an NFA by `PatternParser` + `Automaton`. Partial matches are tracked incrementally using a product graph (`HashProductGraph`) that is the cross-product of the data graph and the automaton states. A match reaches an accepting state when the full pattern is satisfied. |

### Key classes

| Class | Role |
|---|---|
| `Listener` | Hooks into Neo4j's transaction lifecycle |
| `InMemoryOrchestrator` | Lock-free, copy-on-write store of executable triggers |
| `TriggerRegistry` | Thread-safe catalog with snapshot isolation |
| `TriggerRegistryFactory` | Creates an `INDEX` or `AUTOMATON` registry |
| `FullTrigger` | Complete trigger definition (pattern + predicate + action) |
| `PatternParser` | Parses Cypher path patterns into `GraphPath` |
| `Automaton` / `AutomatonTransitionTable` | NFA built from a parsed pattern |
| `HashProductGraph` | Product graph tracking partial NFA matches |
| `AutomatonPathMonitor` | Path matching via the product graph |
| `IndexPathMonitor` | Path matching via the inverted index |

## Dataset

Experiments use the [FinBench](https://ldbcouncil.org/benchmarks/finbench/) synthetic financial graph at two scale factors:

- `sf0.01` — small, used for most experiments
- `sf0.1` — medium, used in `Experiment1Testb`

Incremental data is loaded from pipe-delimited CSV files under `src/test/resources/sf<X>/incremental/`.

The four write queries replayed in each experiment correspond to FinBench transaction workload operations:

| Query | CSV | What it creates |
|---|---|---|
| `tw-1.cypher` | `AddPersonWrite1.csv` | `Person` nodes |
| `tw-4.cypher` | `AddPersonOwnAccountWrite4.csv` | `(Person)-[:own]->(Account)` |
| `tw-6.cypher` | `AddPersonApplyLoanWrite6.csv` | `(Person)-[:apply]->(Loan)` |
| `tw-15.cypher` | `AddLoanDepositAccountWrite15.csv` | `(Loan)-[:deposit]->(Account)` |

`tw-15` is the query that completes the trigger pattern `(:Person)-[:own]->(:Account)<-[:deposit]-(:Loan)` and is therefore the one that causes triggers to fire.

## Building

Requires Java 21 and Maven.

```bash
# Run tests only
mvn test

# Build shaded JAR (for deployment as a Neo4j plugin)
mvn package
```

The `mvn install` target additionally copies the shaded JAR to a local Neo4j installation and runs a deploy script.

## Output format

Each experiment prints a table to stdout:

```
Query         | Mode     | Count | Avg Tx Time (ms) | StdDev Time | Avg Act Latency (ms) | StdDev Latency | Activations | Overhead (%) | Avg Mem (MB) | StdDev Mem
```

- **Avg Tx Time** — average wall-clock time per transaction (execution + commit)
- **Avg Act Latency** — average time from commit start to when the last trigger fires
- **Activations** — total number of trigger callback invocations
- **Overhead** — percentage increase in avg tx time vs the no-trigger baseline

## Experiments

All experiments follow the same structure:
1. **Warmup** — several dry runs to stabilise JVM JIT
2. **Baseline** — full query sequence with no triggers registered
3. **INDEX phase** — same sequence with the INDEX-based registry
4. **PATH phase** — same sequence with the AUTOMATON-based registry

The trigger pattern used unless stated otherwise is:
```
(:Person)-[:own]->(:Account)<-[:deposit]-(:Loan)
```

---

### Experiment 1 — Baseline comparison (`Experiment1Test`, `Experiment1Testb`)

**Goal:** establish the reference performance of each strategy with a single trigger.

| File | Scale factor |
|---|---|
| `Experiment1Test` | sf0.01 |
| `Experiment1Testb` | sf0.1 |

Each run registers **1 trigger** on the real pattern and measures transaction time, activation latency, and memory overhead for both INDEX and AUTOMATON modes.

---

### Experiment 2 — Cascading triggers (`Experiment2Test`, `Experiment2Testb`)

**Goal:** measure the cost of cascading — triggers that themselves produce graph changes that may activate further triggers.

---

### Experiment 3 — Fan-out scaling (`Experiment3Test`)

**Goal:** measure how latency and transaction overhead grow as the number of identical triggers increases.

**Design:** register **N identical triggers** on the same pattern (N ∈ {1, 3, 5, 10}).

| N | Triggers registered | Triggers that fire |
|---|---|---|
| 1 | 1 × real pattern | 1 |
| 3 | 3 × real pattern | 3 |
| 5 | 5 × real pattern | 5 |
| 10 | 10 × real pattern | 10 |

**What to read:** `Avg Act Latency` on `tw-15.cypher` rows — time until the last of the N callbacks completes. Captures both monitoring cost and cumulative firing cost.

---

### Experiment 4 — Monitoring overhead isolation (`Experiment4Test`)

**Goal:** isolate the pure cost of monitoring N registered triggers, independent of how many actually fire.

**Design:** always **1 firing trigger** on the real pattern plus **N−1 silent triggers** on an impossible pattern (`(:Person)-[:own]->(:Person)`, which is never matched by the dataset). Total registered triggers: N ∈ {1, 3, 5, 10}.

| N | Firing | Silent (identical impossible pattern) |
|---|---|---|
| 1 | 1 | 0 |
| 3 | 1 | 2 |
| 5 | 1 | 4 |
| 10 | 1 | 9 |

**What to read:** `Avg Tx Time` and `Overhead` grow purely from monitoring cost, not from firing. `Activations` should stay constant across N.

**Compare with Experiment 3:** the difference reveals how much of Experiment 3's overhead came from firing vs monitoring.
