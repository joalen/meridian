package com.joalen.MostFrequentLongWord;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Final reducer after combiner stage that selects single longest-list word overall
 * 
 */
public class BestWordReducer extends Reducer<Text, Text, Text, IntWritable> {
    private final Text outKey = new Text(); 
    private final IntWritable outVal = new IntWritable(); 

    /** 
     * Finds the pairing of word to count with the highest count among all values 
     * for key and emits it as another pair of word to count.
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{ 
        String bestWord = null;
        int bestCount = -1; 

        for (Text value : values)
        { 
            String[] parts = value.toString().split(",");
            String word = parts[0]; 

            int count = Integer.parseInt(parts[1]);
            if (count > bestCount)
            { 
                bestCount = count; 
                bestWord = word;
            }
        }

        if (bestWord != null)
        { 
            outKey.set(bestWord);
            outVal.set(bestCount);
            context.write(outKey, outVal);
        }
    }
}
