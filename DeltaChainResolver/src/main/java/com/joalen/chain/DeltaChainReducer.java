package com.joalen.chain;

import java.io.IOException;
import java.util.Base64;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.joalen.pack.ChainResolver;

/**
 * One reduce() call = one chain (grouping comparator), with its objects in
 * position order (sort comparator). The reducer reconstructs every object,
 * applying each delta to its already-resolved base, and verifies each one's
 * SHA-1. It still reports the Stage 1 shuffle diagnostics (ordering, positions)
 * so a broken sort or grouping stays visible.
 *
 * Value format from DeltaChainMapper:
 *   sha \t baseSha \t type \t size \t offset \t payloadBase64
 * (a value without the payload column, i.e. topology-only input, is counted in
 * NO_PAYLOAD and skipped rather than failing the job.)
 *
 * Output per chain:
 *   rootSha \t "n=<records> maxPos=<d> ordered=<bool> verified=<k> failed=<f> positions=0x1,1x3,..
 *                [oldest=<iso> newest=<iso> ties=<t>]"
 * (the bracketed part appears only for 9-column input), with
 * "origin=<epoch> patchBytes=<sum> biggestPatch=<size>@<pos>:<sha7>" before it (delta records only).
 * "ordered" means the shuffle delivered (position asc, size DESC, timestamp asc);
 * "ties" counts records whose (position, size, timestamp) equals the previous
 * record's, i.e. the residual order the key cannot decide.
 * plus " firstError=<text>" when something failed.
 *
 * Hadoop detail: while iterating `values`, `key` is updated in place, so
 * key.getChainPosition() changes on every step.
 *
 * Memory: the ChainResolver holds the whole chain's resolved contents until
 * reset() at the next reduce() call. Fine here (largest chain ~0.4 MB); it is
 * the cost Stage 5's bounded top-K is meant to avoid.
 */
public class DeltaChainReducer extends Reducer<ChainKey, Text, Text, Text> {

    public enum ReducerCounters {
        GROUPS, RECORDS, OUT_OF_ORDER_GROUPS,
        VERIFIED, FAILED, DELTAS_APPLIED, NO_PAYLOAD,
        TIED_RECORDS, TIMESTAMPED_RECORDS,
        SIZE_MISMATCH, PATCH_BYTES,
        GROUP_ORDER_VIOLATIONS, ORIGIN_MISMATCH
    }

    private static final int VALUE_FIELDS = 6;

    private final ChainResolver resolver = new ChainResolver();

    // Remembered ACROSS reduce() calls: within one reducer the chains must arrive
    // in ascending (origin, root) order. This is the per-reducer half of the
    // Stage 4 total order; the cross-reducer half is a property of the part files.
    private boolean haveLastGroup;
    private long lastGroupOrigin;
    private String lastGroupRoot;
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(ChainKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String root = key.getChainRootId();
        long origin = key.getChainOrigin(); // key already holds the group's first record
        resolver.reset();

        if (haveLastGroup) {
            int c = Long.compare(origin, lastGroupOrigin);
            if (c == 0) {
                c = root.compareTo(lastGroupRoot);
            }
            if (c <= 0) {
                context.getCounter(ReducerCounters.GROUP_ORDER_VIOLATIONS).increment(1);
            }
        }
        haveLastGroup = true;
        lastGroupOrigin = origin;
        lastGroupRoot = root;

        long records = 0;
        long verified = 0;
        long failed = 0;
        long deltasApplied = 0;
        String firstError = null;

        long lastPos = Long.MIN_VALUE;
        long lastTs = Long.MIN_VALUE;
        long lastSize = Long.MIN_VALUE;
        long sizeMismatch = 0;  // key's size disagreeing with the value's size column
        long patchBytes = 0;    // sum of delta-stream lengths (deltas only)
        long biggestPatch = -1;
        long biggestPatchPos = -1;
        String biggestPatchSha = null;
        long tied = 0;          // records whose (position, timestamp) equals the previous record's
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        long timestamped = 0;
        long runPos = Long.MIN_VALUE;
        long runCount = 0;
        boolean ordered = true;
        StringBuilder runs = new StringBuilder();

        for (Text value : values) {
            long pos = key.getChainPosition(); // key was updated in place for this value
            long ts = key.getObjectTimestamp();
            long size = key.getObjectSize();
            records++;

            // Expected order: position ASCENDING; within a position size
            // DESCENDING; then timestamp ASCENDING. Note the mixed directions:
            // a size going UP inside one position is the violation.
            boolean sameSlot = pos == lastPos;
            if (pos < lastPos
                    || (sameSlot && size > lastSize)
                    || (sameSlot && size == lastSize && ts < lastTs)) {
                ordered = false;
            }
            if (sameSlot && size == lastSize && ts == lastTs) {
                tied++;
            }
            lastPos = pos;
            lastTs = ts;
            lastSize = size;
            if (ts != 0) {
                timestamped++;
                minTs = Math.min(minTs, ts);
                maxTs = Math.max(maxTs, ts);
            }

            if (pos == runPos) {
                runCount++;
            } else {
                if (runCount > 0) {
                    appendRun(runs, runPos, runCount);
                }
                runPos = pos;
                runCount = 1;
            }

            String[] f = value.toString().split("\t", -1);
            if (f.length != VALUE_FIELDS) {
                context.getCounter(ReducerCounters.NO_PAYLOAD).increment(1);
                continue;
            }

            String sha = f[0];
            String baseSha = f[1].equals("-") ? null : f[1];
            String type = f[2];

            // The key was filled in place for THIS value, so its size must equal
            // the value's own size column. If not, key and value got out of step.
            if (!f[3].equals(Long.toString(size))) {
                sizeMismatch++;
            }
            if (baseSha != null) {            // a delta: size is the patch length
                patchBytes += size;
                if (size > biggestPatch) {
                    biggestPatch = size;
                    biggestPatchPos = pos;
                    biggestPatchSha = sha;
                }
            }

            ChainResolver.Result res;
            try {
                long declaredSize = Long.parseLong(f[3]);
                byte[] payload = Base64.getDecoder().decode(f[5]);
                res = resolver.accept(sha, baseSha, type, declaredSize, payload);
            } catch (IllegalArgumentException e) { // bad number or bad base64
                failed++;
                if (firstError == null) firstError = sha + ": bad value (" + e.getMessage() + ")";
                continue;
            }

            if (res.ok) {
                verified++;
                if (res.deltaApplied) deltasApplied++;
            } else {
                failed++;
                if (firstError == null) firstError = sha + ": " + res.error;
            }
        }
        if (runCount > 0) {
            appendRun(runs, runPos, runCount);
        }

        context.getCounter(ReducerCounters.GROUPS).increment(1);
        context.getCounter(ReducerCounters.RECORDS).increment(records);
        context.getCounter(ReducerCounters.VERIFIED).increment(verified);
        context.getCounter(ReducerCounters.FAILED).increment(failed);
        context.getCounter(ReducerCounters.DELTAS_APPLIED).increment(deltasApplied);
        context.getCounter(ReducerCounters.SIZE_MISMATCH).increment(sizeMismatch);
        context.getCounter(ReducerCounters.PATCH_BYTES).increment(patchBytes);
        context.getCounter(ReducerCounters.TIED_RECORDS).increment(tied);
        context.getCounter(ReducerCounters.TIMESTAMPED_RECORDS).increment(timestamped);
        if (!ordered) {
            context.getCounter(ReducerCounters.OUT_OF_ORDER_GROUPS).increment(1);
        }

        outKey.set(root);
        StringBuilder out = new StringBuilder();
        out.append("n=").append(records)
           .append(" maxPos=").append(lastPos)
           .append(" ordered=").append(ordered)
           .append(" verified=").append(verified)
           .append(" failed=").append(failed)
           .append(" positions=").append(runs);
        // origin (the key's chainOrigin) must equal the oldest object timestamp seen in the chain
        if (origin != 0 && timestamped > 0 && origin != minTs) {
            context.getCounter(ReducerCounters.ORIGIN_MISMATCH).increment(1);
        }
        out.append(" origin=").append(origin);
        out.append(" patchBytes=").append(patchBytes);
        if (biggestPatchSha != null) {
            out.append(" biggestPatch=").append(biggestPatch).append('@').append(biggestPatchPos)
               .append(':').append(biggestPatchSha.substring(0, 7));
        }
        if (timestamped > 0) {
            out.append(" oldest=").append(java.time.Instant.ofEpochSecond(minTs))
               .append(" newest=").append(java.time.Instant.ofEpochSecond(maxTs))
               .append(" ties=").append(tied);
        }
        if (firstError != null) {
            out.append(" firstError=").append(firstError.replace('\t', ' ').replace('\n', ' '));
        }
        outVal.set(out.toString());
        context.write(outKey, outVal);
    }

    private static void appendRun(StringBuilder sb, long pos, long count) {
        if (sb.length() > 0) sb.append(',');
        sb.append(pos).append('x').append(count);
    }
}
