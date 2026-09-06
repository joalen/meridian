package com.joalen.InvertedIndex;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reduce;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * Reducer for building inverted index (word -> sorted deduped line numbers)
 */
public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Text result = new Text(); 

    /** 
     * Reduces per-word line numbers into a single deduped, sorted list.
     * For each word, collects all line numbers into a TreeSet (removing
     * duplicates and sorting numerically), then emits them as one comma-separated string.
     * 
     * @param key a word (token) from the mapper
     * @param values possibly duplicated line numbers where that word appeared
     * @param context used to emit pairings of word to "n1, n2, n3, ..."
     * 
     * @throws IOException if system encountered I/O issues
     * @throws InterruptedException if reduce() task gets interruped from system
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    { 
        TreeSet<Integer> lineNumbers = new TreeSet<>(); 

        for (IntWritable value : values)
        { 
            lineNumbers.add(value.get());
        }

        StringBuilder sb = new StringBuilder(); 
        boolean first = true; 

        for (int lineNumber : lineNumbers)
        { 
            if (!first) sb.append(", ");
            sb.append(lineNumber);

            first = false;
        }

        result.set(sb.toString());
        context.write(key, result);
    }
}
