package org.lyon1.path;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;

import java.util.*;

/**
 * Delta-only path matcher:
 *  - Builds canonical signatures for RELATIONSHIP creates/deletes (1-hop)
 *  - Optionally builds 2-hop signatures when two created/deleted relationships share a node
 *
 * No DB traversal is performed (safe w/o Transaction arg).
 * This keeps cost proportional to the size of the tx delta.
 */
public final class DeltaPathMatcher implements PathMatcher {

    /** Toggle 2-hop materialization (kept conservative). */
    private final boolean emitTwoHop;
    /** Guard to avoid O(n^2) blowups on massive txs. */
    private final int twoHopMaxRels;

    public DeltaPathMatcher() {
        this(true, 200); // sensible defaults
    }

    public DeltaPathMatcher(boolean emitTwoHop, int twoHopMaxRels) {
        this.emitTwoHop = emitTwoHop;
        this.twoHopMaxRels = Math.max(0, twoHopMaxRels);
    }

    @Override
    public Set<String> findCanonicalSignatures(TransactionData data, GraphDatabaseService db) {
        // We do not use 'db' here (no traversal); kept for future upgrades.
        LinkedHashSet<String> out = new LinkedHashSet<>();

        // 1) 1-hop signatures from created/deleted rels
        for (Relationship r : data.createdRelationships()) {
            out.add(oneHopSignature(r, Direction.OUTGOING)); // use stored direction start->end
        }
        for (Relationship r : data.deletedRelationships()) {
            out.add(oneHopSignature(r, Direction.OUTGOING));
        }

        // 2) 2-hop (delta-only) if enabled and size is reasonable
        if (emitTwoHop) {
            int total = sizeOf(data.createdRelationships()) + sizeOf(data.deletedRelationships());
            if (total > 0 && total <= twoHopMaxRels) {
                // group by shared node id to quickly find cones
                Map<Long, List<Relationship>> byStart = new HashMap<>();
                Map<Long, List<Relationship>> byEnd   = new HashMap<>();

                for (Relationship r : data.createdRelationships()) {
                    byStart.computeIfAbsent(r.getStartNodeId(), k -> new ArrayList<>()).add(r);
                    byEnd.computeIfAbsent(r.getEndNodeId(),   k -> new ArrayList<>()).add(r);
                }
                for (Relationship r : data.deletedRelationships()) {
                    byStart.computeIfAbsent(r.getStartNodeId(), k -> new ArrayList<>()).add(r);
                    byEnd.computeIfAbsent(r.getEndNodeId(),   k -> new ArrayList<>()).add(r);
                }

                // For each middle node M, create chains X-[]->M-[]->Y and X<-[]-M-[]->Y, etc.
                // We only use the two lists byStart(M) and byEnd(M) to build all directed pairs.
                for (Long m : unionKeys(byStart, byEnd)) {
                    List<Relationship> incomingToM = byEnd.getOrDefault(m, List.of());    // X-[]->M
                    List<Relationship> outgoingFromM = byStart.getOrDefault(m, List.of()); // M-[]->Y

                    // X-[]->M-[]->Y
                    for (Relationship r1 : incomingToM) {
                        for (Relationship r2 : outgoingFromM) {
                            out.add(twoHopSignature(r1, true, r2, true));
                        }
                    }

                    // X<-[]-M-[]->Y  (i.e., M-[]->X AND M-[]->Y through M as start)
                    // Covered by pairs in outgoingFromM if r1 != r2
                    for (int i = 0; i < outgoingFromM.size(); i++) {
                        for (int j = 0; j < outgoingFromM.size(); j++) {
                            if (i == j) continue;
                            Relationship r1 = outgoingFromM.get(i); // M-[]->X
                            Relationship r2 = outgoingFromM.get(j); // M-[]->Y
                            out.add(twoHopSignature(r1, true, r2, true));
                        }
                    }

                    // X-[]->M<-[]-Y  (two incoming into M)
                    for (int i = 0; i < incomingToM.size(); i++) {
                        for (int j = 0; j < incomingToM.size(); j++) {
                            if (i == j) continue;
                            Relationship r1 = incomingToM.get(i); // X-[]->M
                            Relationship r2 = incomingToM.get(j); // Y-[]->M
                            out.add(twoHopSignature(r1, true, r2, false)); // second reversed
                        }
                    }
                }
            }
        }

        return out;
    }

    /* ---------------------- signature helpers ---------------------- */

    /** Canonical 1-hop "(:A&:B)-[:TYPE]->(:C)" */
    private static String oneHopSignature(Relationship r, Direction dir) {
        Node s = r.getStartNode();
        Node e = r.getEndNode();
        // Direction is already implied by start->end in Neo4j core API
        String left  = nodePattern(s);
        String right = nodePattern(e);
        String rel   = relPattern(r);
        return left + "-" + rel + "->" + right;
    }

    /** Canonical 2-hop: leftRel then rightRel, with ability to reverse the right leg. */
    private static String twoHopSignature(Relationship left, boolean leftForward,
                                          Relationship right, boolean rightForward) {
        // middle node is end(left) == start(right) when both forward.
        String leftSrc   = nodePattern(left.getStartNode());
        String leftDst   = nodePattern(left.getEndNode());
        String rightSrc  = nodePattern(right.getStartNode());
        String rightDst  = nodePattern(right.getEndNode());

        String lRel = relPattern(left);
        String rRel = relPattern(right);

        // normalize by ensuring the "middle" textual node is the same label set if possible
        // We still produce a deterministic string without reading any extra graph state.
        if (leftForward && rightForward) {
            // (A)-[L]->(M)-[R]->(B)
            return leftSrc + "-" + lRel + "->" + leftDst + "-" + rRel + "->" + rightDst;
        } else if (leftForward && !rightForward) {
            // (A)-[L]->(M)<-[R]-(B)
            return leftSrc + "-" + lRel + "->" + leftDst + "<-" + rRel + "-" + rightSrc;
        } else if (!leftForward && rightForward) {
            // (A)<-[L]-(M)-[R]->(B)
            return leftDst + "<-" + lRel + "-" + rightSrc + "-" + rRel + "->" + rightDst;
        } else {
            // (A)<-[L]-(M)<-[R]-(B)
            return leftDst + "<-" + lRel + "-" + rightSrc + "<-" + rRel + "-" + rightDst;
        }
    }

    private static String nodePattern(Node n) {
        // Sort labels for canonical representation; empty becomes "()"
        ArrayList<String> labels = new ArrayList<>();
        for (Label l : n.getLabels()) labels.add(l.name());
        if (labels.isEmpty()) return "()";
        Collections.sort(labels);
        StringBuilder sb = new StringBuilder("(:");
        for (int i = 0; i < labels.size(); i++) {
            if (i > 0) sb.append('&');
            sb.append(labels.get(i));
        }
        return sb.append(')').toString();
    }

    private static String relPattern(Relationship r) {
        return "[:"
                + r.getType().name()
                + "]";
    }

    /* ---------------------- utilities ---------------------- */

    private static int sizeOf(Iterable<?> it) {
        int c = 0; for (Object ignored : it) c++; return c;
    }

    private static <K> Set<K> unionKeys(Map<K, ?> a, Map<K, ?> b) {
        Set<K> s = new HashSet<>(a.keySet());
        s.addAll(b.keySet());
        return s;
    }
}
