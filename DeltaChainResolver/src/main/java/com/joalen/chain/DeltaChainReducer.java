package com.joalen.chain;

import java.io.IOException;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DeltaChainReducer extends Reducer<ChainKey, BytesWritable, ChainKey, BytesWritable> {
    @Override
    protected void reduce(ChainKey key, Iterable<BytesWritable> values, Context context)
        throws IOException, InterruptedException {

        throw new UnsupportedOperationException("Todo at the moment");
    }

}
