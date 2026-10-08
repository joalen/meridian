package com.joalen.chain;

import java.io.IOException;
import java.util.Arrays;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DeltaChainReducer extends Reducer<ChainKey, BytesWritable, ChainKey, BytesWritable> {
    @Override
    protected void reduce(ChainKey key, Iterable<BytesWritable> values, Context context)
        throws IOException, InterruptedException {

        byte[] current = null;
        boolean isFirst = true;

        for (BytesWritable value : values) {
            byte[] valueBytes = Arrays.copyOf(value.getBytes(), value.getLength());

            if (isFirst) {
                current = valueBytes;
                isFirst = false;
            } else {
                current = DeltaApplier.apply(current, valueBytes);
            }
        }

        // Emit only the final reconstructed bytes.
        context.write(key, new BytesWritable(current));
    }

}
