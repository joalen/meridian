package com.joalen;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.joalen.DistinctCharacters.DistinctCountCombiner;
import com.joalen.DistinctCharacters.DistinctCountReducer;
import com.joalen.DistinctCharacters.LengthLastCharMapper;

public class Q1Analysis {
    private static final Pattern NON_LETTER = Pattern.compile("[^a-z]+");

    /** 
     * Mapper stage in MapReduce for Part 1A that helps aggregate word to frequency provided some com.google.thirdparty.publicsuffix
     */
    static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();
    
        
        /** 
         * Builds map for all alphabetical words found in a text corpus
         * 
         * @param key input record key
         * @param value input record value 
         * @param context MapReduce context to store key-value pairings for entire MapReduce lifecycle
         * 
         * @throws IOException I/O errors from system 
         * @throws InterruptedException if system interrupts MapReduce's map() functionality
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        { 
            String line = value.toString().toLowerCase();
            String[] tokens = NON_LETTER.split(line);
    
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

    /** 
     * Reducer stage in MapReduce for Part 1A and 1B that does, combiner stage (summed frequencies) + "shuffle and sorting" + reduction. 
     * Once reduced, there's the word to frequency mapping.
     */
    static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable(); 

        /** 
         * Adds together all values received from Mapper provided same key and yields out the output for 
         * key and total sum of frequencies together. 
         * 
         * @param key word associated for values to sum 
         * @param values collection of integer values to sum for a word 
         * @param context MapReduce's context from the Mapper stage for ALL global key-value maps
         * 
         * @throws IOException system errored due to I/O
         * @throws InterruptedException system interrupted the reduce() operation for some reason
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        { 
            int sum = 0; 

            for (IntWritable value : values)
            { 
                sum += value.get(); 
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    /** 
     * Mapper stage in MapReducer for Part 1B that is similar to WordsMapper BUT filters even further with a provided list
     */
    static class TargetWordsMapper extends Mapper<Object, Text, Text, IntWritable> {
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
            String[] tokens = NON_LETTER.split(line);
    
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
            case "B": 
                job.setMapperClass(TargetWordsMapper.class);
                job.setCombinerClass(SumReducer.class);
                job.setReducerClass(SumReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);   
                break; 
            case "C": 
                job.setMapperClass(LengthLastCharMapper.class);
                job.setCombinerClass(DistinctCountCombiner.class);
                job.setReducerClass(DistinctCountReducer.class);
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