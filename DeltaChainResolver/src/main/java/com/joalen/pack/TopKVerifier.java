package com.joalen.pack;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Standalone check of the Stage 5 job output against the input TSV, computed the
 * slow independent way: for every chain, collect all its deltas, SORT them all,
 * and take the first K (the thing the reducer's bounded queue avoids doing).
 *
 * Usage: TopKVerifier <topology.tsv> <merged-output.txt> <K>
 *
 *   topology.tsv        any of the 7-10 column pre-pass files (needs columns 1,2,4,6)
 *   merged-output.txt   cat of the job's part-r-* files
 *
 * Compares, per chain: deltas, kept, patchBytes, topBytes and the exact top= list.
 * Also checks every chain appears exactly once and no unknown chain appears.
 */
public final class TopKVerifier {

    private static final class Delta {
        long size;
        long pos;
        String sha;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: TopKVerifier <topology.tsv> <merged-output.txt> <K>");
            System.exit(2);
        }
        int k = Integer.parseInt(args[2]);

        Map<String, List<Delta>> deltas = new HashMap<>();
        Map<String, Integer> records = new HashMap<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length < 7) throw new IOException("bad TSV line: " + line.substring(0, Math.min(80, line.length())));
                String root = f[0];
                records.merge(root, 1, Integer::sum);
                if (!deltas.containsKey(root)) deltas.put(root, new ArrayList<Delta>());
                if (!f[3].equals("-")) {
                    Delta d = new Delta();
                    d.pos = Long.parseLong(f[1]);
                    d.sha = f[2];
                    d.size = Long.parseLong(f[5]);
                    deltas.get(root).add(d);
                }
            }
        }

        Set<String> seen = new HashSet<>();
        long chains = 0;
        long bad = 0;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[1]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                chains++;
                String[] kv = line.split("\t", 2);
                String root = kv[0];
                Map<String, String> v = fields(kv[1]);

                if (!seen.add(root)) { bad++; report(root, "appears twice"); continue; }
                List<Delta> all = deltas.get(root);
                if (all == null) { bad++; report(root, "not in the input"); continue; }

                Collections.sort(all, new Comparator<Delta>() {
                    @Override
                    public int compare(Delta a, Delta b) {
                        int c = Long.compare(b.size, a.size);
                        if (c != 0) return c;
                        c = Long.compare(a.pos, b.pos);
                        if (c != 0) return c;
                        return a.sha.compareTo(b.sha);
                    }
                });
                int keep = Math.min(k, all.size());
                long patchBytes = 0;
                for (Delta d : all) patchBytes += d.size;
                long topBytes = 0;
                StringBuilder top = new StringBuilder();
                for (int i = 0; i < keep; i++) {
                    Delta d = all.get(i);
                    topBytes += d.size;
                    if (i > 0) top.append(',');
                    top.append(d.size).append('@').append(d.pos).append(':').append(d.sha, 0, 7);
                }
                String wantTop = keep == 0 ? "-" : top.toString();

                String problem = null;
                if (Long.parseLong(v.get("n")) != records.get(root)) problem = "n=" + v.get("n") + " want " + records.get(root);
                else if (Long.parseLong(v.get("deltas")) != all.size()) problem = "deltas=" + v.get("deltas") + " want " + all.size();
                else if (Integer.parseInt(v.get("kept")) != keep) problem = "kept=" + v.get("kept") + " want " + keep;
                else if (Long.parseLong(v.get("patchBytes")) != patchBytes) problem = "patchBytes=" + v.get("patchBytes") + " want " + patchBytes;
                else if (Long.parseLong(v.get("topBytes")) != topBytes) problem = "topBytes=" + v.get("topBytes") + " want " + topBytes;
                else if (!v.get("top").equals(wantTop)) problem = "top=" + v.get("top") + " want " + wantTop;
                if (problem != null) { bad++; report(root, problem); }
            }
        }
        if (seen.size() != deltas.size()) {
            bad++;
            System.out.println("MISSING CHAINS: output has " + seen.size() + ", input has " + deltas.size());
        }

        if (bad == 0) {
            System.out.println("ALL " + chains + " CHAINS MATCH (top-" + k + " recomputed by sort-then-truncate)");
        } else {
            System.out.println(bad + " PROBLEMS");
            System.exit(1);
        }
    }

    private static Map<String, String> fields(String s) {
        Map<String, String> m = new HashMap<>();
        for (String tok : s.trim().split(" ")) {
            int eq = tok.indexOf('=');
            if (eq > 0) m.put(tok.substring(0, eq), tok.substring(eq + 1));
        }
        return m;
    }

    private static void report(String root, String msg) {
        System.out.println("MISMATCH " + root + ": " + msg);
    }

    private TopKVerifier() {}
}
