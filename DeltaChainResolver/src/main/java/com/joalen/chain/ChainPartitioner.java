package com.joalen.chain;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ChainPartitioner extends Partitioner<ChainKey, Writable> {
    @Override
    public int getPartition(ChainKey chainKey, Writable writable, int numPartitions) {
        return (chainKey.getChainRootId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
