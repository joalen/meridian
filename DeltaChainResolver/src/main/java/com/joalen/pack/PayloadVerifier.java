package com.joalen.pack;

import java.io.BufferedReader;
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

/**
 * Standalone (no Hadoop, NO .pack file) check that a topology TSV with a payload
 * column is self-sufficient: everything a reducer needs to reconstruct and
 * verify every object is on the line itself.
 *
 * Line format (TopologyPrePass with a pack argument):
 *   rootSha  position  sha  baseSha  type  size  offset  payloadBase64
 *
 * This is the local mirror of the reducer: it feeds each chain's objects, in
 * position order, to the same ChainResolver the reducer uses.
 *
 * Usage: PayloadVerifier <topology-with-payload.tsv>
 */
public final class PayloadVerifier {

    private static final class Row {
        long position;
        String sha;
        String baseSha;   // null for roots
        String type;
        long size;        // root: object size; delta: delta-stream length
        byte[] payload;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: PayloadVerifier <topology-with-payload.tsv>");
            System.exit(2);
        }

        Map<String, List<Row>> chains = new LinkedHashMap<>();
        long total = 0;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length != 8) {
                    throw new IOException("expected 8 columns (is the payload column present?): got " + f.length);
                }
                Row r = new Row();
                r.position = Long.parseLong(f[1]);
                r.sha = f[2];
                r.baseSha = f[3].equals("-") ? null : f[3];
                r.type = f[4];
                r.size = Long.parseLong(f[5]);
                r.payload = Base64.getDecoder().decode(f[7]);
                List<Row> list = chains.get(f[0]);
                if (list == null) {
                    list = new ArrayList<>();
                    chains.put(f[0], list);
                }
                list.add(r);
                total++;
            }
        }

        long ok = 0;
        long failed = 0;
        long deltasApplied = 0;
        ChainResolver resolver = new ChainResolver();

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
                if (res.ok) {
                    ok++;
                    if (res.deltaApplied) deltasApplied++;
                } else {
                    failed++;
                    if (failed <= 10) {
                        System.err.println("FAIL " + r.sha + " (pos " + r.position + "): " + res.error);
                    }
                }
            }
        }

        System.out.println("objects:        " + total);
        System.out.println("chains:         " + chains.size());
        System.out.println("deltas applied: " + deltasApplied);
        System.out.println("verified OK:    " + ok);
        System.out.println("failed:         " + failed);
        System.out.println(failed == 0 && ok == total ? "ALL OBJECTS VERIFIED FROM TSV ALONE" : "VERIFICATION FAILED");
        System.exit(failed == 0 && ok == total ? 0 : 1);
    }

    private PayloadVerifier() {}
}
