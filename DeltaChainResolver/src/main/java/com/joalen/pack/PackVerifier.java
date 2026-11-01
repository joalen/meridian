package com.joalen.pack;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Standalone (no Hadoop) end-to-end check of PackReader + DeltaApplier.
 *
 * Reads TopologyPrePass output, and for every chain root processes its objects
 * in position order exactly the way the reducer will: keep a sha -> content
 * map for the current chain, resolve each delta against its base, then verify
 *
 *     sha1("<type> <size>\0" + content) == the object's sha
 *
 * Sizes: verify-pack's size column is the full-object size for a chain root but
 * the length of the inflated DELTA STREAM for a delta object (the resolved size
 * is not listed), so each is checked against the right quantity. The SHA-1 check
 * is what proves the resolved content. Memory is released between chains,
 * mirroring one reduce() call at a time.
 *
 * Usage: PackVerifier <file.pack> <topology.tsv>
 */
public final class PackVerifier {

    private static final class Row {
        String sha;
        String baseSha;   // null for roots
        String type;
        long size;        // root: object size; delta: inflated delta-stream length
        long offset;
        long position;
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        if (args.length != 2) {
            System.err.println("Usage: PackVerifier <file.pack> <topology.tsv>");
            System.exit(2);
        }

        // Group rows by chain root (insertion order does not matter).
        Map<String, List<Row>> chains = new LinkedHashMap<>();
        long total = 0;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(args[1]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length != 7) throw new IOException("bad topology line: " + line);
                Row r = new Row();
                r.position = Long.parseLong(f[1]);
                r.sha = f[2];
                r.baseSha = f[3].equals("-") ? null : f[3];
                r.type = f[4];
                r.size = Long.parseLong(f[5]);
                r.offset = Long.parseLong(f[6]);
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
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");

        try (PackReader pack = new PackReader(Paths.get(args[0]))) {
            for (List<Row> chain : chains.values()) {
                // The reducer receives these already position-ordered; here we sort.
                Collections.sort(chain, new Comparator<Row>() {
                    @Override
                    public int compare(Row a, Row b) {
                        return Long.compare(a.position, b.position);
                    }
                });

                Map<String, byte[]> resolved = new HashMap<>();

                for (Row r : chain) {
                    try {
                        PackReader.RawObject raw = pack.readAt(r.offset);
                        byte[] content;

                        if (r.baseSha == null) {
                            if (raw.isDelta()) {
                                throw new IllegalStateException("topology says root, pack says delta");
                            }
                            content = raw.data;
                            if (content.length != r.size) {
                                throw new IllegalStateException("size " + content.length + " != expected " + r.size);
                            }
                        } else {
                            if (!raw.isDelta()) {
                                throw new IllegalStateException("topology says delta, pack says full object");
                            }
                            byte[] base = resolved.get(r.baseSha);
                            if (base == null) {
                                throw new IllegalStateException("base " + r.baseSha
                                        + " not resolved yet (position order broken?)");
                            }
                            if (raw.data.length != r.size) {
                                throw new IllegalStateException("delta stream " + raw.data.length
                                        + " bytes != expected " + r.size);
                            }
                            content = DeltaApplier.apply(base, raw.data);
                            deltasApplied++;
                        }

                        resolved.put(r.sha, content);

                        String actual = hex(hashObject(sha1, r.type, content));
                        if (!actual.equals(r.sha)) {
                            throw new IllegalStateException("sha mismatch, computed " + actual);
                        }
                        ok++;
                    } catch (RuntimeException | IOException e) {
                        failed++;
                        if (failed <= 10) {
                            System.err.println("FAIL " + r.sha + " (pos " + r.position + ", "
                                    + r.type + "): " + e.getMessage());
                        }
                    }
                }
                // `resolved` goes out of scope here: one chain's memory at a time.
            }
        }

        System.out.println("objects:        " + total);
        System.out.println("chains:         " + chains.size());
        System.out.println("deltas applied: " + deltasApplied);
        System.out.println("verified OK:    " + ok);
        System.out.println("failed:         " + failed);
        System.out.println(failed == 0 && ok == total ? "ALL OBJECTS VERIFIED" : "VERIFICATION FAILED");
        System.exit(failed == 0 && ok == total ? 0 : 1);
    }

    /** sha1 over the git object header ("<type> <size>\0") followed by the content. */
    static byte[] hashObject(MessageDigest md, String type, byte[] content) {
        md.reset();
        md.update((type + " " + content.length).getBytes(StandardCharsets.US_ASCII));
        md.update((byte) 0);
        md.update(content);
        return md.digest();
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        }
        return sb.toString();
    }

    private PackVerifier() {}
}
