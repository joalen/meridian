package com.joalen.chain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Standalone check (no Hadoop, no cluster) that BoundedTopK gives exactly what
 * "sort everything, keep the first K" gives, for random data with many ties,
 * every K from 1 to beyond the item count, and random arrival orders.
 *
 * Usage: java -cp DeltaChainResolver.jar com.joalen.chain.BoundedTopKSelfCheck
 */
public final class BoundedTopKSelfCheck {

    private static final Comparator<long[]> BEST_FIRST = new Comparator<long[]>() {
        // item = {size, position, id}: size descending, then position ascending, then id ascending
        @Override
        public int compare(long[] a, long[] b) {
            int c = Long.compare(b[0], a[0]);
            if (c != 0) return c;
            c = Long.compare(a[1], b[1]);
            if (c != 0) return c;
            return Long.compare(a[2], b[2]);
        }
    };

    public static void main(String[] args) {
        Random rnd = new Random(7);
        int trials = 0;
        for (int n : new int[] {0, 1, 2, 5, 17, 100, 1000}) {
            for (int round = 0; round < 40; round++) {
                List<long[]> items = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    // small ranges on purpose, so sizes and positions tie a lot
                    items.add(new long[] {rnd.nextInt(8), rnd.nextInt(5), i});
                }
                List<long[]> expected = new ArrayList<>(items);
                Collections.sort(expected, BEST_FIRST);

                for (int k : new int[] {1, 2, 3, 5, 10, n, n + 3}) {
                    if (k < 1) continue;
                    List<long[]> shuffled = new ArrayList<>(items);
                    Collections.shuffle(shuffled, rnd);
                    BoundedTopK<long[]> top = new BoundedTopK<>(k, BEST_FIRST);
                    for (long[] it : shuffled) {
                        top.offer(it);
                        if (top.size() > k) {
                            throw new AssertionError("queue grew past k=" + k);
                        }
                    }
                    List<long[]> got = top.toSortedList();
                    List<long[]> want = expected.subList(0, Math.min(k, n));
                    if (got.size() != want.size()) {
                        throw new AssertionError("size " + got.size() + " != " + want.size());
                    }
                    for (int i = 0; i < got.size(); i++) {
                        if (BEST_FIRST.compare(got.get(i), want.get(i)) != 0) {
                            throw new AssertionError("mismatch at n=" + n + " k=" + k + " rank " + i);
                        }
                    }
                    // offered = kept-now + evicted + rejected
                    if (top.offered() != top.size() + top.evicted() + top.rejected()) {
                        throw new AssertionError("counter identity broken");
                    }
                    trials++;
                }
            }
        }
        System.out.println("BoundedTopK matches sort-then-truncate in " + trials + " randomized trials");
    }

    private BoundedTopKSelfCheck() {}
}
