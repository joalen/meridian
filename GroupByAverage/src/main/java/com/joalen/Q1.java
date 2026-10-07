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


public class Q1 
{
    private static final int REGION = 0; 
    private static final int STATE = 2; 
    private static final int YEAR = 6; 
    private static final int TEMP = 7;
    private static final double INVALID = -99.0;

    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text>
    { 
        private final Text outKey = new Text(); 
        private final Text outVal = new Text(); 

        @Override 
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String csvLine = value.toString(); 
            if (csvLine.isEmpty()) return; 

            String[] row = csvLine.split(", ", -1);
            if (row.length <= TEMP) return;

            String region = row[REGION], state = row[STATE], year = row[YEAR];
            
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
            outVal.set(temperature + "1, ");
            context.write(outKey, outVal);
        }
    }

    public static class SumCountCombiner extends Reducer<Text, Text, Text, Text>
    { 
        private final Text outVal = new Text(); 
        
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

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> 
    { 
        private final Text outVal = new Text(); 

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
            System.err.println("Usage: Q1 <in> <out>");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];

        Job job = Job.getInstance(config, "Q1");
        job.setJarByClass(Q1.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
