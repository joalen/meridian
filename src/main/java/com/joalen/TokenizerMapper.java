package com.joalen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    { 
        String line = value.toString().toLowerCase();
        String[] tokens = line.split("[^a-z]+"); // non-alphabet splits 

        for (String token : tokens)
        { 
            if (!token.isEmpty())
            { 
                word.set(token);
                context.write(word, ONE);
            }
        }
    }
}
