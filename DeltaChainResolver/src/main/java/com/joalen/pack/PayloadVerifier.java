package com.joalen.pack;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
 * This is the local mirror of the Part 4 reducer. For each chain it walks
 * objects in position order, keeps a sha -> content map, applies each delta to
 * its base, and checks sha1("<type> <size>\0" + content) == sha.
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

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
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
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");

        for (List<Row> chain : chains.values()) {
            Collections.sort(chain, new Comparator<Row>() {
                @Override
                public int compare(Row a, Row b) {
                    return Long.compare(a.position, b.position);
                }
            });

            Map<String, byte[]> resolved = new HashMap<>();
            for (Row r : chain) {
                try {
                    if (r.payload.length != r.size) {
                        throw new IllegalStateException("payload " + r.payload.length
                                + " bytes != expected " + r.size);
                    }
                    byte[] content;
                    if (r.baseSha == null) {
                        content = r.payload;
                    } else {
                        byte[] base = resolved.get(r.baseSha);
                        if (base == null) {
                            throw new IllegalStateException("base " + r.baseSha + " not resolved yet");
                        }
                        content = DeltaApplier.apply(base, r.payload);
                    }
                    resolved.put(r.sha, content);

                    String actual = PackVerifier.hex(PackVerifier.hashObject(sha1, r.type, content));
                    if (!actual.equals(r.sha)) {
                        throw new IllegalStateException("sha mismatch, computed " + actual);
                    }
                    ok++;
                } catch (RuntimeException e) {
                    failed++;
                    if (failed <= 10) {
                        System.err.println("FAIL " + r.sha + " (pos " + r.position + "): " + e.getMessage());
                    }
                }
            }
        }

        System.out.println("objects:     " + total);
        System.out.println("chains:      " + chains.size());
        System.out.println("verified OK: " + ok);
        System.out.println("failed:      " + failed);
        System.out.println(failed == 0 && ok == total ? "ALL OBJECTS VERIFIED FROM TSV ALONE" : "VERIFICATION FAILED");
        System.exit(failed == 0 && ok == total ? 0 : 1);
    }

    private PayloadVerifier() {}
}
