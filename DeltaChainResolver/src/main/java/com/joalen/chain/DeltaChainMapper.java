package com.joalen.chain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads one line of TopologyPrePass output per object:
 *
 *   rootSha  position  sha  baseSha  type  size  offset
 *
 * and emits (ChainKey(rootSha, position), "sha \t baseSha \t type \t size \t offset").
 */
public class DeltaChainMapper extends Mapper<LongWritable, Text, ChainKey, Text> {
    private static final int FIELDS = 7;

    public enum MapperCounters { RECORDS, MALFORMED }

    private final ChainKey outKey = new ChainKey();
    private final Text outVal = new Text();

    /**
     * @param key byte offset of the line (unused)
     * @param value one topology line
     * @param context used to emit (ChainKey, payload)
     *
     * @throws IOException system encounters an I/O error
     * @throws InterruptedException Mapper task interrupted from system
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;

        String[] f = line.split("\t", -1);
        if (f.length != FIELDS) {
            context.getCounter(MapperCounters.MALFORMED).increment(1);
            return;
        }

        long position;
        try {
            position = Long.parseLong(f[1]);
        } catch (NumberFormatException nfe) {
            context.getCounter(MapperCounters.MALFORMED).increment(1);
            return;
        }

        outKey.set(f[0], position);
        // sha, baseSha, type, size, offset
        outVal.set(f[2] + "\t" + f[3] + "\t" + f[4] + "\t" + f[5] + "\t" + f[6]);

        context.getCounter(MapperCounters.RECORDS).increment(1);
        context.write(outKey, outVal);
    }
}
