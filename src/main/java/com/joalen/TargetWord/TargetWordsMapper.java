package com.joalen.TargetWord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TargetWordsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text word = new Text(); 

    private static final Set<String> TARGETS = new HashSet<>(Arrays.asList( 
        "ahab",
        "captain",
        "harpoon"
    ));

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    { 
        String line = value.toString().toLowerCase(); 
        String[] tokens = line.split("[^a-z]+");

        for (String token : tokens)
        { 
            if (TARGETS.contains(token))
            { 
                word.set(token);
                context.write(word, ONE);
            }
        }
    }
}
