package com.joalen.chain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Decides WHICH REDUCER a record goes to. Hashes chainRootId only, so every
 * record of one chain lands on the same reducer no matter its chainPosition.
 *
 * With Hadoop's default HashPartitioner the whole ChainKey (position
 * included) would be hashed and one chain would be scattered across
 * reducers.
 *
 * This is separate from grouping (which records share a reduce() call) and
 * sorting (what order they arrive in).
 */
public class ChainPartitioner extends Partitioner<ChainKey, Text> {
    @Override
    public int getPartition(ChainKey key, Text value, int numPartitions) {
        // mask the sign bit instead of Math.abs (abs(Integer.MIN_VALUE) is negative)
        return (key.getChainRootId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
