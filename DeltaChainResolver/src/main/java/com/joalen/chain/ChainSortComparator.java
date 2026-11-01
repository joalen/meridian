package com.joalen.chain;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Decides the ORDER keys are sorted in before they reach the reducer. This is
 * the third of the three separate shuffle classes:
 *
 *   ChainPartitioner         which reducer a key goes to      (root)
 *   ChainSortComparator      the order records are sorted in  (origin asc, root asc, position asc, size DESC, timestamp asc)
 *   ChainGroupingComparator  which sorted neighbors form one  (root)
 *                            reduce() call
 *
 * Within one chain this puts records in dependency order (position ascending,
 * so a base always arrives before the deltas that need it). Among records at the
 * same position the order is MIXED-DIRECTION: largest size first (descending),
 * then oldest first (ascending). Records at one position never depend on each
 * other, so this extra ordering is free to choose; it makes the biggest patch
 * at each depth arrive first. Registered with job.setSortComparatorClass.
 *
 * It must agree with the grouping comparator on one point: all keys of one chain
 * must sort next to each other, otherwise one chain would be cut into several
 * groups. They do because origin is a function of root (every record of a chain
 * has the same origin), so (origin, root) never interleaves two chains.
 * TotalOrderPartitioner also uses THIS comparator to place keys in ranges.
 */
public class ChainSortComparator extends WritableComparator {
    public ChainSortComparator() {
        super(ChainKey.class, true); // true: instantiate keys so compare() gets objects
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        ChainKey left = (ChainKey) a;
        ChainKey right = (ChainKey) b;

        // Stage 4: the chain's origin time leads, so chains order against each
        // other (this is the field a range partitioner cuts on). It is the same
        // for every record of a chain, so it never separates one chain's records.
        int c = Long.compare(left.getChainOrigin(), right.getChainOrigin());
        if (c != 0) {
            return c;
        }
        c = left.getChainRootId().compareTo(right.getChainRootId());
        if (c != 0) {
            return c;
        }
        c = Long.compare(left.getChainPosition(), right.getChainPosition());
        if (c != 0) {
            return c;
        }
        // Descending: swap the operands. Do NOT write `right - left`; subtraction
        // can overflow for large values and flips the sign of the result.
        c = Long.compare(right.getObjectSize(), left.getObjectSize());
        if (c != 0) {
            return c;
        }
        return Long.compare(left.getObjectTimestamp(), right.getObjectTimestamp());
    }
}
