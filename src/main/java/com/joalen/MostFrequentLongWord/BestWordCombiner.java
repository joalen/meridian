package com.joalen.MostFrequentLongWord;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * Combiner to help locally reduce pairings of word to count down to a single pair with highest 
 * count for each key found.
 */
public class BestWordCombiner extends Reducer<Text, Text, Text, Text> {
    private final Text outVal = new Text(); 

    /** 
     * Finds the pairings of word to count, where it has highest count among all values for 
     * key and emits only that one
     * 
     * @param key shared constant key from mapper stage
     * @param values pairings of word to count for comparison 
     * @param context MapReduce mapper that contains the next payload to emit key to bestWord and bestCount 
     * 
     * @throws IOException system halts for I/O issues
     * @throws InterruptedException reduce() stage interrupted by system  
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
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
            outVal.set(bestWord + "," + bestCount);
            context.write(key, outVal);
        }
    }
}
