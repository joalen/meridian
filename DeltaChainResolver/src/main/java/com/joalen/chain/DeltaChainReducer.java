package com.joalen.chain;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * DIAGNOSTIC reducer for Stage 1. It does not reconstruct objects yet; it
 * checks that the shuffle machinery did its job:
 *
 *  - one reduce() call per chain root       (grouping comparator works)
 *  - positions arrive in non-decreasing order (sort comparator works)
 *  - a root never shows up on two reducers   (partitioner works; check by
 *                                             comparing GROUPS to the number of roots)
 *
 * Output per chain:  rootSha  TAB  "n=<records> maxPos=<d> ordered=<bool> positions=0x1,1x3,..."
 * where positions is run-length encoded ("<position>x<count>").
 *
 * IMPORTANT Hadoop detail: while iterating `values`, Hadoop mutates `key` in
 * place, so key.getChainPosition() changes on every step. That is how you
 * read each record's position inside one grouped reduce() call.
 */
public class DeltaChainReducer extends Reducer<ChainKey, Text, Text, Text> {

    public enum ReducerCounters { GROUPS, RECORDS, OUT_OF_ORDER_GROUPS }

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void reduce(ChainKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String root = key.getChainRootId();

        long records = 0;
        long lastPos = Long.MIN_VALUE;
        long runPos = Long.MIN_VALUE;
        long runCount = 0;
        boolean ordered = true;
        StringBuilder runs = new StringBuilder();

        for (Text ignored : values) {
            long pos = key.getChainPosition(); // key was updated in place for this value
            records++;

            if (pos < lastPos) {
                ordered = false;
            }
            lastPos = pos;

            if (pos == runPos) {
                runCount++;
            } else {
                if (runCount > 0) {
                    appendRun(runs, runPos, runCount);
                }
                runPos = pos;
                runCount = 1;
            }
        }
        if (runCount > 0) {
            appendRun(runs, runPos, runCount);
        }

        context.getCounter(ReducerCounters.GROUPS).increment(1);
        context.getCounter(ReducerCounters.RECORDS).increment(records);
        if (!ordered) {
            context.getCounter(ReducerCounters.OUT_OF_ORDER_GROUPS).increment(1);
        }

        outKey.set(root);
        outVal.set("n=" + records + " maxPos=" + lastPos + " ordered=" + ordered + " positions=" + runs);
        context.write(outKey, outVal);
    }

    private static void appendRun(StringBuilder sb, long pos, long count) {
        if (sb.length() > 0) sb.append(',');
        sb.append(pos).append('x').append(count);
    }
}
