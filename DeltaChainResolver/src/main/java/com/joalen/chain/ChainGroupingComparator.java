package com.joalen.chain;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Decides WHICH KEYS SHARE ONE reduce() CALL. Compares chainRootId only, so
 * all positions of a chain arrive as one ordered Iterable.
 *
 * If this compared the full key, every distinct (root, position) would get
 * its own reduce() call with a single value and nothing would error.
 */
public class ChainGroupingComparator extends WritableComparator {
    public ChainGroupingComparator() {
        super(ChainKey.class, true); // true: instantiate keys so compare() gets objects
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
        ChainKey left = (ChainKey) a;
        ChainKey right = (ChainKey) b;
        return left.getChainRootId().compareTo(right.getChainRootId());
    }
}
