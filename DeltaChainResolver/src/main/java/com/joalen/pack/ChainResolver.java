package com.joalen.pack;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reconstructs and verifies the objects of ONE delta chain (really a delta
 * tree: several objects can share a base). No Hadoop dependencies, so the same
 * code runs in the reducer and in the local PayloadVerifier.
 *
 * Contract: feed it the chain's objects in non-decreasing position order, so
 * every delta's base was accepted before the delta itself. It keeps a
 * sha -> content map for the current chain, which is the "reducer holds the
 * whole tree in memory" cost; call reset() between chains.
 *
 * For each object accept() checks:
 *   - payload length equals the size recorded for it (root: object size,
 *     delta: delta-stream length, as `git verify-pack` reports them)
 *   - roots: content = payload; deltas: content = DeltaApplier.apply(base, payload)
 *   - sha1("<type> <len>\0" + content) equals the object's sha
 */
public final class ChainResolver {

    /** Outcome of one accept() call. */
    public static final class Result {
        public final boolean ok;
        public final boolean deltaApplied;
        public final String error;   // null when ok

        private Result(boolean ok, boolean deltaApplied, String error) {
            this.ok = ok;
            this.deltaApplied = deltaApplied;
            this.error = error;
        }
    }

    private final Map<String, byte[]> resolved = new HashMap<>();
    private final MessageDigest sha1;

    public ChainResolver() {
        try {
            this.sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 not available", e);
        }
    }

    /** Forget everything from the previous chain. */
    public void reset() {
        resolved.clear();
    }

    /** Number of objects currently held (useful for memory reasoning). */
    public int heldObjects() {
        return resolved.size();
    }

    /**
     * @param sha      the object's id
     * @param baseSha  its base's id, or null for a chain root
     * @param type     "commit", "tree", "blob" or "tag"
     * @param size     expected payload length (see class comment)
     * @param payload  inflated bytes as stored in the pack
     */
    public Result accept(String sha, String baseSha, String type, long size, byte[] payload) {
        try {
            if (payload.length != size) {
                return fail("payload " + payload.length + " bytes != expected " + size);
            }

            byte[] content;
            boolean applied = false;
            if (baseSha == null) {
                content = payload;
            } else {
                byte[] base = resolved.get(baseSha);
                if (base == null) {
                    return fail("base " + baseSha + " not resolved (missing or failed earlier)");
                }
                content = DeltaApplier.apply(base, payload);
                applied = true;
            }

            sha1.reset();
            sha1.update((type + " " + content.length).getBytes(StandardCharsets.US_ASCII));
            sha1.update((byte) 0);
            sha1.update(content);
            String actual = hex(sha1.digest());
            if (!actual.equals(sha)) {
                return fail("sha mismatch, computed " + actual);
            }

            resolved.put(sha, content);   // only verified content becomes a base
            return new Result(true, applied, null);
        } catch (RuntimeException e) {
            return fail(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static Result fail(String why) {
        return new Result(false, false, why);
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        }
        return sb.toString();
    }
}
