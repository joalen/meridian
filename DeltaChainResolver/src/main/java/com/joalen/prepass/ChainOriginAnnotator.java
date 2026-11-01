package com.joalen.prepass;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Adds the 10th column (chainOrigin, epoch seconds) to a timestamped TSV.
 *
 *   in:  rootSha position sha baseSha type size offset payloadBase64 objectTimestamp
 *   out: ... objectTimestamp chainOrigin
 *
 * chainOrigin is the OLDEST object timestamp in the chain (the earliest moment
 * any object of this chain entered history), repeated on every row of the chain.
 * Chains with no timestamped object get 0.
 *
 * Why a pre-pass and not the mapper: a mapper sees one line at a time and cannot
 * know its chain's minimum. The total-order sort key needs a value that is
 * identical for every record of a chain (otherwise a chain's records would be
 * scattered across reducers), so it has to be computed over the whole chain first.
 *
 * Two passes over the file: the first builds root -> min timestamp (small),
 * the second rewrites each line.
 *
 * Usage: ChainOriginAnnotator <in.tsv (9 cols)> <out.tsv>
 */
public final class ChainOriginAnnotator {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: ChainOriginAnnotator <in.tsv> <out.tsv>");
            System.exit(2);
        }

        Map<String, Long> origin = new HashMap<>();
        long lines = 0;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length != 9) {
                    throw new IOException("expected 9 columns (run TimestampAnnotator first), got " + f.length);
                }
                long ts = Long.parseLong(f[8]);
                lines++;
                Long cur = origin.get(f[0]);
                if (ts != 0 && (cur == null || cur == 0 || ts < cur)) {
                    origin.put(f[0], ts);
                } else if (cur == null) {
                    origin.put(f[0], 0L);
                }
            }
        }

        long minO = Long.MAX_VALUE;
        long maxO = Long.MIN_VALUE;
        for (long o : origin.values()) {
            minO = Math.min(minO, o);
            maxO = Math.max(maxO, o);
        }

        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]), StandardCharsets.UTF_8);
             BufferedWriter out = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                int tab = line.indexOf('\t');
                out.write(line);
                out.write('\t');
                out.write(Long.toString(origin.get(line.substring(0, tab))));
                out.write('\n');
            }
        }

        System.out.println("rows:                 " + lines);
        System.out.println("chains:               " + origin.size());
        System.out.println("chain origin range:   " + java.time.Instant.ofEpochSecond(minO)
                + "  /  " + java.time.Instant.ofEpochSecond(maxO));
    }

    private ChainOriginAnnotator() {}
}
