package com.joalen.chain;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Sends every key belonging to the same chain to the same reducer,
 * regardless of chainPosition. Without this, Hadoop's default
 * HashPartitioner would hash the whole ChainKey (position included) and
 * scatter one chain's deltas across multiple reducers, which may break the
 * entire premise of streaming a chain through in order.
 */
public class ChainPartitioner extends Partitioner<ChainKey, Writable> {
    @Override
    public int getPartition(ChainKey chainKey, Writable writable, int numPartitions) {
        return (chainKey.getChainRootId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
