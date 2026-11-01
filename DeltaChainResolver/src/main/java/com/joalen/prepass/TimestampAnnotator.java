package com.joalen.prepass;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.joalen.pack.ChainResolver;
import com.joalen.pack.ObjectTimes;

/**
 * Adds the 9th column (objectTimestamp, epoch seconds) to a payload TSV.
 *
 *   in:  rootSha position sha baseSha type size offset payloadBase64
 *   out: rootSha position sha baseSha type size offset payloadBase64 objectTimestamp
 *
 * The timestamp is when the object entered history: the committer time of the
 * earliest commit that reaches it (see ObjectTimes). To know that, every commit
 * and tree has to be reconstructed, so this reuses ChainResolver exactly like
 * the reducer does, keeps the commit/tree/tag contents in memory, and walks them.
 * Needs only the TSV (no .pack file). Objects no commit reaches get 0.
 *
 * Usage: TimestampAnnotator <in.tsv> <out.tsv>
 */
public final class TimestampAnnotator {

    private static final class Row {
        String line;
        long position;
        String sha;
        String baseSha;
        String type;
        long size;
        byte[] payload;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: TimestampAnnotator <in.tsv> <out.tsv>");
            System.exit(2);
        }

        List<Row> all = new ArrayList<>();
        Map<String, List<Row>> chains = new LinkedHashMap<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length != 8) {
                    throw new IOException("expected 8 columns (payload TSV), got " + f.length);
                }
                Row r = new Row();
                r.line = line;
                r.position = Long.parseLong(f[1]);
                r.sha = f[2];
                r.baseSha = f[3].equals("-") ? null : f[3];
                r.type = f[4];
                r.size = Long.parseLong(f[5]);
                r.payload = Base64.getDecoder().decode(f[7]);
                all.add(r);
                List<Row> list = chains.get(f[0]);
                if (list == null) {
                    list = new ArrayList<>();
                    chains.put(f[0], list);
                }
                list.add(r);
            }
        }

        // Reconstruct, keeping the contents that history-walking needs.
        ObjectTimes.Contents contents = new ObjectTimes.Contents();
        ChainResolver resolver = new ChainResolver();
        long failed = 0;
        for (List<Row> chain : chains.values()) {
            Collections.sort(chain, new Comparator<Row>() {
                @Override
                public int compare(Row a, Row b) {
                    return Long.compare(a.position, b.position);
                }
            });
            resolver.reset();
            for (Row r : chain) {
                ChainResolver.Result res = resolver.accept(r.sha, r.baseSha, r.type, r.size, r.payload);
                if (!res.ok) {
                    failed++;
                    continue;
                }
                if (r.type.equals("commit")) {
                    contents.commits.put(r.sha, resolver.get(r.sha));
                } else if (r.type.equals("tree")) {
                    contents.trees.put(r.sha, resolver.get(r.sha));
                } else if (r.type.equals("tag")) {
                    contents.tags.put(r.sha, resolver.get(r.sha));
                }
            }
        }
        if (failed > 0) {
            throw new IOException(failed + " objects failed verification; run PayloadVerifier first");
        }

        ObjectTimes.Result times = ObjectTimes.compute(contents);

        long unreached = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        try (BufferedWriter out = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8)) {
            for (Row r : all) {
                Long t = times.time.get(r.sha);
                long ts = t == null ? 0 : t;
                if (t == null) {
                    unreached++;
                } else {
                    minTime = Math.min(minTime, ts);
                    maxTime = Math.max(maxTime, ts);
                }
                out.write(r.line);
                out.write('\t');
                out.write(Long.toString(ts));
                out.write('\n');
            }
        }

        System.out.println("objects:               " + all.size());
        System.out.println("commits parsed:        " + times.commits
                + (times.unparsableCommits > 0 ? "  (unparsable: " + times.unparsableCommits + ")" : ""));
        System.out.println("objects with a time:   " + (all.size() - unreached));
        System.out.println("unreached (time = 0):  " + unreached);
        if (minTime != Long.MAX_VALUE) {
            System.out.println("earliest / latest:     " + java.time.Instant.ofEpochSecond(minTime)
                    + "  /  " + java.time.Instant.ofEpochSecond(maxTime));
        }
    }

    private TimestampAnnotator() {}
}
