package com.joalen.chain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads one line of pre-pass output per object. Four shapes are accepted:
 *
 *   7 cols  rootSha position sha baseSha type size offset                       (topology only)
 *   8 cols  ... offset payloadBase64                                            (Part 3)
 *   9 cols  ... offset payloadBase64 objectTimestamp                            (Stage 2)
 *  10 cols  ... objectTimestamp chainOrigin                                     (Stage 4)
 *
 * and emits (ChainKey(chainOrigin, rootSha, position, timestamp, size), "sha \t baseSha \t type \t size \t offset [\t payload]").
 * Timestamp and chainOrigin go into the KEY (so the shuffle can sort and
 * range-partition by them) and are removed from the value; when a column is
 * absent its value is 0.
 */
public class DeltaChainMapper extends Mapper<LongWritable, Text, ChainKey, Text> {
    private static final int FIELDS_TOPOLOGY = 7;
    private static final int FIELDS_WITH_PAYLOAD = 8;
    private static final int FIELDS_WITH_TIMESTAMP = 9;
    private static final int FIELDS_WITH_ORIGIN = 10;

    public enum MapperCounters { RECORDS, MALFORMED, WITH_PAYLOAD, WITH_TIMESTAMP, WITH_ORIGIN }

    private final ChainKey outKey = new ChainKey();
    private final Text outVal = new Text();

    /**
     * @param key byte offset of the line (unused)
     * @param value one pre-pass line
     * @param context used to emit (ChainKey, value)
     *
     * @throws IOException system encounters an I/O error
     * @throws InterruptedException Mapper task interrupted from system
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;

        String[] f = line.split("\t", -1);
        if (f.length != FIELDS_TOPOLOGY && f.length != FIELDS_WITH_PAYLOAD
                && f.length != FIELDS_WITH_TIMESTAMP && f.length != FIELDS_WITH_ORIGIN) {
            context.getCounter(MapperCounters.MALFORMED).increment(1);
            return;
        }

        long position;
        long timestamp = 0;
        long origin = 0;
        long size;
        try {
            position = Long.parseLong(f[1]);
            size = Long.parseLong(f[5]);
            if (f.length >= FIELDS_WITH_TIMESTAMP) {
                timestamp = Long.parseLong(f[8]);
            }
            if (f.length == FIELDS_WITH_ORIGIN) {
                origin = Long.parseLong(f[9]);
            }
        } catch (NumberFormatException nfe) {
            context.getCounter(MapperCounters.MALFORMED).increment(1);
            return;
        }

        outKey.set(origin, f[0], position, timestamp, size); // size stays in the value too
        // sha, baseSha, type, size, offset [, payload]
        String payload = f.length >= FIELDS_WITH_PAYLOAD ? "\t" + f[7] : "";
        outVal.set(f[2] + "\t" + f[3] + "\t" + f[4] + "\t" + f[5] + "\t" + f[6] + payload);

        if (f.length >= FIELDS_WITH_PAYLOAD) {
            context.getCounter(MapperCounters.WITH_PAYLOAD).increment(1);
        }
        if (f.length >= FIELDS_WITH_TIMESTAMP) {
            context.getCounter(MapperCounters.WITH_TIMESTAMP).increment(1);
        }
        if (f.length == FIELDS_WITH_ORIGIN) {
            context.getCounter(MapperCounters.WITH_ORIGIN).increment(1);
        }

        context.getCounter(MapperCounters.RECORDS).increment(1);
        context.write(outKey, outVal);
    }
}
