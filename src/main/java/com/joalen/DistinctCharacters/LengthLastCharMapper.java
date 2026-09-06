package com.joalen.DistinctCharacters;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LengthLastCharMapper extends Mapper<Object, Text, Text, Text> {
    private final Text outKey = new Text(); 
    private final Text outVal = new Text(); 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    { 
        String line = value.toString().toLowerCase(); 
        String[] tokens = line.split("[^a-z]+");

        for (String token : tokens)
        { 
            if (!token.isEmpty())
            { 
                char lastChar = token.charAt(token.length() - 1);
                outKey.set(token.length() + "," + lastChar);
                outVal.set(token);

                context.write(outKey, outVal);
            }
        }
    }
}
