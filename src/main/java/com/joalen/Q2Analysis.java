package com.joalen;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.joalen.InvertedIndex.InvertedIndexReducer;
import com.joalen.MostFrequentLongWord.BestWordCombiner;
import com.joalen.MostFrequentLongWord.BestWordReducer;
import com.joalen.MostFrequentLongWord.LongWordMapper;

public class Q2Analysis {
    /** 
     * Mapper stage for MapReduce that helps build an inverted index from Q2 dataset
     */
    static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final int[] INDEXED_FIELDS = {1, 2, 4, 5};
        private static final Pattern NON_LETTER = Pattern.compile("[^a-z]+");

        private final Text word = new Text(); 
        private final IntWritable lineNumber = new IntWritable(); 
        
        /** 
         * Mapping function to take in q2's dataset to where it emits a mapping of token to lineNumber 
         * pairs for each word found in columns of dataset's indexed fields. This accounts skipping 
         * header rows and any malformed lines (like non-numerical lines or lines with fewer than 10 fields)
         * 
         * @param key ignored (byte offset of the line, per TextInputFormat)
         * @param value one line from Q2 dataset 
         * @param context emitted MapReduce pairings of token to lineNumber 
         * 
         * @throws IOException system encounters an I/O error 
         * @throws InterruptedException Mapper task from MapReduce interrupted from system
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
            String[] fields = value.toString().split(",", -1); // no empty state columns 

            if (fields.length < 10) { 
                return;
            }

            int lineNumberFromDataset; 
            try { 
                lineNumberFromDataset = Integer.parseInt(fields[0].trim());
            } catch (NumberFormatException nfe) { 
                return;
            }

            lineNumber.set(lineNumberFromDataset);

            for (int index : INDEXED_FIELDS) { 
                String fieldValue = fields[index].trim().toLowerCase(); 

                if (fieldValue.isEmpty()) { 
                    continue;
                }
                
                String[] tokens = NON_LETTER.split(fieldValue);

                for (String token : tokens) { 
                    if (!token.isEmpty()) { 
                        word.set(token);
                        context.write(word, lineNumber);
                    }
                }
            }
        }
    }

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
