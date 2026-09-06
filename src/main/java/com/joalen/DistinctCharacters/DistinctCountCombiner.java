package com.joalen.DistinctCharacters;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctCountCombiner extends Reducer<Text, Text, Text, Text> {
    private final Text outVal = new Text(); 

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    { 
        Set<String> distinctWords = new HashSet<>(); 
        
        for (Text value : values)
        { 
            distinctWords.add(value.toString());
        }

        for (String distinctWord : distinctWords) 
        { 
            outVal.set(distinctWord);
            context.write(key, outVal);
        }
    }
}
