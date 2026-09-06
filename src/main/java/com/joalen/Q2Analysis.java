package com.joalen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.joalen.InvertedIndex.InvertedIndexMapper;
import com.joalen.InvertedIndex.InvertedIndexReducer;
import com.joalen.MostFrequentLongWord.BestWordCombiner;
import com.joalen.MostFrequentLongWord.BestWordReducer;
import com.joalen.MostFrequentLongWord.LongWordMapper;

public class Q2Analysis {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException { 
        Configuration config = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs(); 

        if (otherArgs.length != 3) { 
            System.err.println("Usage: Q2Analysis <in> <out> <part>");
            System.err.println("part: A (InvertedIndex) or B (MostFrequentLongWord)");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];
        String part = otherArgs[2];

        Job job = Job.getInstance(config, "Q2 Analysis - Part " + part);
        job.setJarByClass(Q2Analysis.class);

        switch (part) { 
            case "A": 
                job.setMapperClass(InvertedIndexMapper.class);
                job.setReducerClass(InvertedIndexReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case "B": 
                job.setMapperClass(LongWordMapper.class);
                job.setCombinerClass(BestWordCombiner.class);
                job.setReducerClass(BestWordReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;
            default:
                System.err.println("Part " + part + " not implemented yet.");
                System.exit(2);
        }

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
