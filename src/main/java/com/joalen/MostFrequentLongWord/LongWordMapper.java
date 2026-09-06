package com.joalen.MostFrequentLongWord;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * Filters the inverted index from previously having made it from the dataset for Q2 down to 
 * "long" words and counts occurences of them.
 */
public class LongWordMapper extends Mapper<Object, Text, Text, Text> {
    private static final int MIN_LENGTH = 12; 
    private static final Text CONSTANT_KEY = new Text("candidate");
    
    private final Text outVal = new Text(); 

    /**
     * Parses the output from having transformed dataset into pairings generated from part 1 of Q2
     * and firstly does parsing of tab-separated word to lines. Then, it drops pairings if malformed
     * via shorter than min length or if no tab is found. 
     * 
     * This emits pairings of word to count
     *  
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); 
        int tabIndex = line.indexOf('\t');

        if (tabIndex < 0) { 
            return;
        }

        String word = line.substring(0, tabIndex);

        if (word.length() < MIN_LENGTH) { 
            return;
        }

        String listPart = line.substring(tabIndex+1).trim();
        int count = countLineNumbers(listPart);
        
        outVal.set(word + ", " + count);
        context.write(CONSTANT_KEY, outVal);
    }

    private int countLineNumbers(String listPart) { 
        if (listPart.isEmpty()) { 
            return 0;
        }

        return listPart.split(",\\s*").length;
    }
}
