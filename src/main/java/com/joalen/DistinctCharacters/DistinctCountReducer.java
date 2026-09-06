package com.joalen.DistinctCharacters;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctCountReducer extends Reducer<Text, Text, Text, IntWritable> {
    private final IntWritable result = new IntWritable(); 

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    { 
        Set<String> distinctWords = new HashSet<>(); 
        
        for (Text value : values)
        { 
            distinctWords.add(value.toString());
        }

        result.set(distinctWords.size());
        context.write(key, result);
    }
}
