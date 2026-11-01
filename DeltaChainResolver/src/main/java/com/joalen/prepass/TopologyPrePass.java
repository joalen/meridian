package com.joalen.prepass;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.joalen.pack.PackReader;

/**
 * Local (non-Hadoop) pre-pass. Reads the output of `git verify-pack -v` and
 * writes one tab-separated line per object:
 *
 *   rootSha  position  sha  baseSha  type  size  offset  [payload]
 *
 * baseSha is "-" for non-delta objects (chain roots, position 0).
 *
 * If a .pack file is given as a third argument, each line also gets a payload
 * column: base64 of the object's inflated bytes exactly as stored in the pack
 * (full content for a chain root, the delta instruction stream for a delta).
 * Base64 never contains a tab or newline, so the TSV stays one record per line.
 *
 * Also validates its own work against verify-pack's depth column and the
 * "non delta" / "chain length = d" summary lines.
 *
 * Usage: TopologyPrePass <verify.txt> <out.tsv> [file.pack]
 */
public final class TopologyPrePass {

    private static final Pattern NON_DELTA = Pattern.compile("^non delta: (\\d+) objects?$");
    private static final Pattern HISTO = Pattern.compile("^chain length = (\\d+): (\\d+) objects?$");

    private static final class Entry {
        String sha;
        String type;
        long size;
        long offset;
        String baseSha;      // null for non-delta
        int reportedDepth;   // 0 for non-delta
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2 && args.length != 3) {
            System.err.println("Usage: TopologyPrePass <verify.txt> <out.tsv> [file.pack]");
            System.exit(2);
        }

        Map<String, Entry> entries = new LinkedHashMap<>();
        long reportedNonDelta = -1;
        Map<Integer, Long> reportedHisto = new TreeMap<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if (isObjectLine(line)) {
                    Entry e = parseObjectLine(line);
                    entries.put(e.sha, e);
                    continue;
                }

                Matcher m = NON_DELTA.matcher(line);
                if (m.matches()) {
                    reportedNonDelta = Long.parseLong(m.group(1));
                    continue;
                }
                m = HISTO.matcher(line);
                if (m.matches()) {
                    reportedHisto.put(Integer.parseInt(m.group(1)), Long.parseLong(m.group(2)));
                }
                // anything else (e.g. "...pack: ok") is ignored
            }
        }

        // Resolve root + depth for every object, memoized so shared chain
        // prefixes are only walked once.
        Map<String, String> rootOf = new HashMap<>();
        Map<String, Integer> depthOf = new HashMap<>();
        for (String sha : entries.keySet()) {
            resolve(sha, entries, rootOf, depthOf);
        }

        // Write output + collect our own stats.
        long computedNonDelta = 0;
        Map<Integer, Long> computedHisto = new TreeMap<>();
        int depthMismatches = 0;

        PackReader pack = args.length == 3 ? new PackReader(Paths.get(args[2])) : null;
        long payloadBytes = 0;

        try (BufferedWriter out = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8)) {
            for (Entry e : entries.values()) {
                int depth = depthOf.get(e.sha);
                if (depth != e.reportedDepth) {
                    depthMismatches++;
                    if (depthMismatches <= 10) {
                        System.err.println("DEPTH MISMATCH " + e.sha + ": computed " + depth
                                + " verify-pack " + e.reportedDepth);
                    }
                }
                if (depth == 0) {
                    computedNonDelta++;
                } else {
                    computedHisto.merge(depth, 1L, Long::sum);
                }

                out.write(rootOf.get(e.sha));
                out.write('\t');
                out.write(Integer.toString(depth));
                out.write('\t');
                out.write(e.sha);
                out.write('\t');
                out.write(e.baseSha == null ? "-" : e.baseSha);
                out.write('\t');
                out.write(e.type);
                out.write('\t');
                out.write(Long.toString(e.size));
                out.write('\t');
                out.write(Long.toString(e.offset));
                if (pack != null) {
                    PackReader.RawObject raw = pack.readAt(e.offset);
                    if (raw.isDelta() != (e.baseSha != null)) {
                        throw new IllegalStateException("pack and verify.txt disagree about " + e.sha
                                + " (is the .pack the same one verify.txt was made from?)");
                    }
                    payloadBytes += raw.data.length;
                    out.write('\t');
                    out.write(Base64.getEncoder().encodeToString(raw.data));
                }
                out.write('\n');
            }
        } finally {
            if (pack != null) {
                pack.close();
            }
        }

        // Validation report.
        System.out.println("objects parsed:        " + entries.size());
        System.out.println("depth mismatches:      " + depthMismatches);
        if (pack != null) {
            System.out.println("payload bytes (raw):   " + payloadBytes);
        }
        System.out.println("non-delta (computed):  " + computedNonDelta
                + "   (verify-pack: " + reportedNonDelta + ")");
        boolean histoOk = computedHisto.equals(reportedHisto);
        System.out.println("chain-length histogram matches verify-pack: " + histoOk);
        if (!histoOk) {
            for (Map.Entry<Integer, Long> r : reportedHisto.entrySet()) {
                Long mine = computedHisto.get(r.getKey());
                if (mine == null || !mine.equals(r.getValue())) {
                    System.out.println("  depth " + r.getKey() + ": computed " + mine
                            + ", verify-pack " + r.getValue());
                }
            }
        }
        boolean ok = depthMismatches == 0 && histoOk && computedNonDelta == reportedNonDelta;
        System.out.println(ok ? "ALL CHECKS PASSED" : "CHECKS FAILED");
        System.exit(ok ? 0 : 1);
    }

    /**
     * Walks up base pointers from `sha` until it reaches an object whose root
     * is already known (or a non-delta object), then fills in root/depth for
     * the whole path on the way back down.
     */
    private static void resolve(String sha, Map<String, Entry> entries,
                                Map<String, String> rootOf, Map<String, Integer> depthOf) {
        if (rootOf.containsKey(sha)) return;

        Deque<String> path = new ArrayDeque<>();
        String cur = sha;

        while (!rootOf.containsKey(cur)) {
            Entry e = entries.get(cur);
            if (e == null) {
                throw new IllegalStateException("base object " + cur + " not found in pack listing");
            }
            if (e.baseSha == null) {
                rootOf.put(cur, cur);
                depthOf.put(cur, 0);
                break;
            }
            path.push(cur);
            cur = e.baseSha;
        }

        // pop() yields the entry closest to the already-resolved base first.
        while (!path.isEmpty()) {
            String s = path.pop();
            String parent = entries.get(s).baseSha;
            rootOf.put(s, rootOf.get(parent));
            depthOf.put(s, depthOf.get(parent) + 1);
        }
    }

    /** Object lines start with a 40-char hex sha followed by whitespace. */
    private static boolean isObjectLine(String line) {
        if (line.length() < 41 || !Character.isWhitespace(line.charAt(40))) return false;
        for (int i = 0; i < 40; i++) {
            if (Character.digit(line.charAt(i), 16) < 0) return false;
        }
        return true;
    }

    /** sha type size size-in-pack offset [depth base-sha] */
    private static Entry parseObjectLine(String line) {
        String[] f = line.split("\\s+");
        if (f.length != 5 && f.length != 7) {
            throw new IllegalArgumentException("unexpected field count " + f.length + ": " + line);
        }
        Entry e = new Entry();
        e.sha = f[0];
        e.type = f[1];
        e.size = Long.parseLong(f[2]);
        // f[3] = size in pack (unused)
        e.offset = Long.parseLong(f[4]);
        if (f.length == 7) {
            e.reportedDepth = Integer.parseInt(f[5]);
            e.baseSha = f[6];
        }
        return e;
    }

    private TopologyPrePass() {}
}
