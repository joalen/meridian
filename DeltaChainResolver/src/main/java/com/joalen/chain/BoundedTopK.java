package com.joalen.chain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Keeps the K best items seen so far, no matter how many are offered, in
 * O(log K) per item and O(K) memory. No Hadoop dependencies.
 *
 * The queue is a binary heap whose HEAD is the WORST item currently kept. A new
 * item is compared against only that head:
 *
 *   - queue not full yet          -> add it
 *   - better than the head        -> evict the head, add it       (head moves up)
 *   - not better than the head    -> discard it; it can never matter again
 *
 * The ranking is maintained incrementally as items arrive. Nothing is ever
 * sorted in full and then truncated (that would hold every item at once and cost
 * O(n log n)); only the final K items are sorted, once, when the result is read.
 *
 * "Better" is defined by the comparator passed in (negative = first is better).
 * That comparator should be a TOTAL order (no ties) so the result does not depend
 * on arrival order.
 */
public final class BoundedTopK<T> {
    private final int k;
    private final Comparator<T> bestFirst;
    private final PriorityQueue<T> worstAtHead;

    private long offered;
    private long evicted;   // entered the queue, later pushed out by a better item
    private long rejected;  // never entered: not better than the K already kept

    public BoundedTopK(int k, Comparator<T> bestFirst) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1, got " + k);
        }
        this.k = k;
        this.bestFirst = bestFirst;
        // java.util.PriorityQueue puts the SMALLEST element (per its comparator) at the head.
        // Reversing "best first" makes the smallest element the worst item.
        this.worstAtHead = new PriorityQueue<T>(k + 1, Collections.reverseOrder(bestFirst));
    }

    /** @return true if the item entered the queue */
    public boolean offer(T item) {
        offered++;
        if (worstAtHead.size() < k) {
            worstAtHead.add(item);
            return true;
        }
        if (bestFirst.compare(item, worstAtHead.peek()) < 0) {
            worstAtHead.poll();
            worstAtHead.add(item);
            evicted++;
            return true;
        }
        rejected++;
        return false;
    }

    public int size() {
        return worstAtHead.size();
    }

    public long offered() {
        return offered;
    }

    public long evicted() {
        return evicted;
    }

    public long rejected() {
        return rejected;
    }

    /** The kept items, best first. Sorts only the (at most K) kept items. */
    public List<T> toSortedList() {
        List<T> list = new ArrayList<>(worstAtHead);
        Collections.sort(list, bestFirst);
        return list;
    }

    /** Empties the queue and resets the counters so one instance serves many groups. */
    public void clear() {
        worstAtHead.clear();
        offered = 0;
        evicted = 0;
        rejected = 0;
    }
}
