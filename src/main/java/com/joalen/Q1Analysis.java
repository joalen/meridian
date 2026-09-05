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

import com.joalen.WordCount.SumReducer;
import com.joalen.WordCount.WordCountMapper;

public class Q1Analysis {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    { 
        Configuration config = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: Q1Analysis <in> <out> <part>");
            System.err.println("part: A (WordCount), B (TargetWords), or C (Pattern)");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];
        String part = otherArgs[2];

        Job job = Job.getInstance(config, "Q1 Analysis - Part " + part);
        job.setJarByClass(Q1Analysis.class);

        switch (part)
        { 
            case "A": 
                job.setMapperClass(WordCountMapper.class);
                job.setCombinerClass(SumReducer.class);
                job.setReducerClass(SumReducer.class);
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
