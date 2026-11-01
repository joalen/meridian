package com.joalen.chain;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Stage 5: for each chain, report only the K most expensive deltas (the largest
 * patches), using a bounded priority queue instead of holding the whole chain.
 *
 * Contrast with DeltaChainReducer (Stages 1-4): that one reconstructs every
 * object, so it keeps the whole chain's contents in memory and needs every
 * payload through the shuffle. This one never reconstructs anything. The ranking
 * input is the patch size, which is already in the key, so:
 *
 *   - memory per chain is O(K), not O(chain size)
 *   - the mapper can drop the payload from the shuffle (see DeltaChainMapper)
 *
 * The price: it no longer proves the objects rebuild (SHA-1 verification). That
 * was Stages 1-4's job; this stage is the cost report.
 *
 * Enabled by -Dchain.topk=K (K >= 1) on the command line.
 *
 * Ranking (a total order, so the result never depends on arrival order):
 * larger patch first; ties -> lower chain position first; then smaller sha.
 *
 * Output per chain:
 *   rootSha \t "n=<records> deltas=<d> k=<K> kept=<j> patchBytes=<all deltas> topBytes=<kept deltas>
 *               top=<size>@<pos>:<sha7>,... origin=<epoch>"
 * (top=- when the chain has no deltas; a root is not a delta, it has no patch.)
 */
public class ChainTopKReducer extends Reducer<ChainKey, Text, Text, Text> {

    public enum TopKCounters {
        GROUPS, RECORDS, DELTAS, ACCEPTED, EVICTED, REJECTED,
        PATCH_BYTES, TOP_BYTES, SIZE_MISMATCH, MALFORMED, GROUP_ORDER_VIOLATIONS
    }

    /** One delta's cost. Immutable. */
    private static final class Patch {
        final long size;
        final long position;
        final String sha;

        Patch(long size, long position, String sha) {
            this.size = size;
            this.position = position;
            this.sha = sha;
        }
    }

    private static final Comparator<Patch> BEST_FIRST = new Comparator<Patch>() {
        @Override
        public int compare(Patch a, Patch b) {
            int c = Long.compare(b.size, a.size);            // bigger patch first
            if (c != 0) return c;
            c = Long.compare(a.position, b.position);        // then shallower first
            if (c != 0) return c;
            return a.sha.compareTo(b.sha);                   // then by sha: a total order
        }
    };

    private int k;
    private BoundedTopK<Patch> top;
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    private boolean haveLastGroup;
    private long lastGroupOrigin;
    private String lastGroupRoot;

    @Override
    protected void setup(Context context) {
        k = context.getConfiguration().getInt("chain.topk", 5);
        if (k < 1) {
            throw new IllegalArgumentException("chain.topk must be >= 1, got " + k);
        }
        top = new BoundedTopK<Patch>(k, BEST_FIRST);
    }

    @Override
    protected void reduce(ChainKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String root = key.getChainRootId();
        long origin = key.getChainOrigin();
        top.clear();

        if (haveLastGroup) {
            int c = Long.compare(origin, lastGroupOrigin);
            if (c == 0) {
                c = root.compareTo(lastGroupRoot);
            }
            if (c <= 0) {
                context.getCounter(TopKCounters.GROUP_ORDER_VIOLATIONS).increment(1);
            }
        }
        haveLastGroup = true;
        lastGroupOrigin = origin;
        lastGroupRoot = root;

        long records = 0;
        long deltas = 0;
        long patchBytes = 0;
        long sizeMismatch = 0;
        long malformed = 0;

        for (Text value : values) {
            records++;
            long size = key.getObjectSize();      // key updated in place for this value
            long pos = key.getChainPosition();

            // value: sha, baseSha, type, size, offset [, payload] -- payload may be absent
            String[] f = value.toString().split("\t", -1);
            if (f.length < 5) {
                malformed++;
                continue;
            }
            if (!f[3].equals(Long.toString(size))) {
                sizeMismatch++;
            }
            if (f[1].equals("-")) {
                continue;                         // a root: no patch, not a candidate
            }
            deltas++;
            patchBytes += size;
            top.offer(new Patch(size, pos, f[0]));
        }

        List<Patch> kept = top.toSortedList();
        long topBytes = 0;
        StringBuilder list = new StringBuilder();
        for (Patch p : kept) {
            topBytes += p.size;
            if (list.length() > 0) list.append(',');
            list.append(p.size).append('@').append(p.position).append(':').append(p.sha, 0, 7);
        }

        context.getCounter(TopKCounters.GROUPS).increment(1);
        context.getCounter(TopKCounters.RECORDS).increment(records);
        context.getCounter(TopKCounters.DELTAS).increment(deltas);
        context.getCounter(TopKCounters.ACCEPTED).increment(top.size() + top.evicted());
        context.getCounter(TopKCounters.EVICTED).increment(top.evicted());
        context.getCounter(TopKCounters.REJECTED).increment(top.rejected());
        context.getCounter(TopKCounters.PATCH_BYTES).increment(patchBytes);
        context.getCounter(TopKCounters.TOP_BYTES).increment(topBytes);
        context.getCounter(TopKCounters.SIZE_MISMATCH).increment(sizeMismatch);
        context.getCounter(TopKCounters.MALFORMED).increment(malformed);

        outKey.set(root);
        outVal.set("n=" + records + " deltas=" + deltas + " k=" + k + " kept=" + kept.size()
                + " patchBytes=" + patchBytes + " topBytes=" + topBytes
                + " top=" + (list.length() == 0 ? "-" : list) + " origin=" + origin);
        context.write(outKey, outVal);
    }
}
