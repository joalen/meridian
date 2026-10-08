package com.joalen.chain;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Decides on which keys are considered within the "same group" for a single
 * reduce() operation. Only compares on chainRootId, where if accidentally
 * comparing full key (including chainPosition), then every delta in chain
 * gets its reduce() call with one value rather than one reduce() call per
 * chain and deltas as an Iterable.
 */
public class ChainGroupingComparator extends WritableComparator {
    protected ChainGroupingComparator() {
        super(ChainKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ChainKey k1 = (ChainKey) a;
        ChainKey k2 = (ChainKey) b;

        return k1.getChainRootId().compareTo(k2.getChainRootId());
    }
}
