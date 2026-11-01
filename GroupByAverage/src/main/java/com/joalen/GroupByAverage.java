package com.joalen;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class GroupByAverage 
{
    private static final int REGION = 0; 
    private static final int STATE = 2; 
    private static final int YEAR = 6; 
    private static final int TEMP = 7;
    private static final double INVALID = -99.0;

    /** 
     * Parses out dataset.csv and yields partial (sum, count) pairs for West-region readings.
     */
    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text>
    { 
        private final Text outKey = new Text(); 
        private final Text outVal = new Text(); 

        /** 
         * Transforms a single CSV line from the dataset.csv
         * 
         * @param key byte offset of the line in the input file (unused)
         * @param value one line of the CSV file
         * @param context needed to help emit the final (state/year, temperature/count) pair
         * 
         * @throws IOException system encounters an I/O error 
         * @throws InterruptedException Mapper task from MapReduce interrupted from system
         */
        @Override 
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String csvLine = value.toString(); 
            if (csvLine.isEmpty()) return; 

            String[] row = csvLine.split(",", -1);
            if (row.length <= TEMP) return;

            String region = row[REGION].trim(), state = row[STATE].trim(), year = row[YEAR].trim();
            
            // no headers
            if (region.equals("Region")) return;

            // exact matches
            if (!region.equals("West")) return;

            if (state.isEmpty() || year.isEmpty()) return;

            double temperature; 
            try { 
                temperature = Double.parseDouble(row[TEMP]);
            } catch (NumberFormatException nfe)
            { 
                return;
            }

            if (temperature == INVALID) return; 

            outKey.set(state + "\t" + year);
            outVal.set(temperature + ",1");
            context.write(outKey, outVal);
        }
    }

    /** 
     * A combiner that merges those partial sum, count pairings from the mapper step into a single 
     * entity reprensentation of sum,count per key to alleviate shuffle traffic 
     */
    public static class SumCountCombiner extends Reducer<Text, Text, Text, Text>
    { 
        private final Text outVal = new Text(); 
        
        /** 
         * Adds up partial sums and counts into a single (state, year) kotlin.system
         * 
         * @param key the {@code "state\tyear"} key from mapper step
         * @param values {@code "sum,count"} strings from the mapper or earlier combiner runs
         * @param context needed to help emit the combined {@code "sum,count"} value
         * 
         * @throws IOException system encounters an I/O error 
         * @throws InterruptedException Combiner task from MapReduce interrupted from system
         */
        @Override 
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        { 
            double sum = 0;
            long count = 0; 

            for (Text value : values)
            { 
                String[] parts = value.toString().split(",");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }

            outVal.set(sum + "," + count);
            context.write(key, outVal);
        }
    }

    /** 
     * Reducer in GroupByAverage that totals all the {@code "sum,count"} values for a (state, year) and returns back 
     * mean temperature per state and year
     */
    public static class AverageReducer extends Reducer<Text, Text, Text, Text> 
    { 
        private final Text outVal = new Text(); 

        /**
         * Computes {@code sum / count} for one (state, year) key.
         * 
         * @param key the {@code "state\tyear"} key
         * @param values {@code "sum,count"} strings to aggregate
         * @param context used to emit the formatted average
         * 
         * @throws IOException system encounters an I/O error 
         * @throws InterruptedException Reducer task from MapReduce interrupted from system
         */
        @Override 
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        { 
            double sum = 0;
            long count = 0; 

            for (Text value : values)
            { 
                String[] parts = value.toString().split(",");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }

            if (count == 0) return;
            outVal.set(String.format(Locale.US, "%.2f", sum / count));
            context.write(key, outVal);
        }
    }

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration config = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs(); 

        if (otherArgs.length != 2) { 
            System.err.println("Usage: GroupByAverage <in> <out>");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];

        Job job = Job.getInstance(config, "GroupByAverage");
        job.setJarByClass(GroupByAverage.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(in, "dataset.csv"));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
