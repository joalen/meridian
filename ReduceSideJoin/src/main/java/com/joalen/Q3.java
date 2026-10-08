package com.joalen;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.naming.Context;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q3 
{
    // columns city_temperatures.csv
    private static final int REGION = 0;
    private static final int STATE = 2;
    private static final int CITY = 3;
    private static final int YEAR = 6;
    private static final int TEMP = 7;
    private static final double INVALID = -99.0;

    // columns state-capital.csv
    private static final int CAP_STATE = 0;
    private static final int CAP_CAPITAL = 1;
    private static final int CAP_TYPE = 2;

    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text>
    { 
        private final Text outKey = new Text(); 
        private final Text outVal = new Text(); 

        @Override 
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String csvLine = value.toString(); 
            if (csvLine.isEmpty()) return; 

            String[] row = csvLine.split(",", -1);
            if (row.length <= TEMP) return;

            String region = row[REGION].trim(), state = row[STATE].trim(), city = row[CITY].trim(), year = row[YEAR].trim();
            
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

            outKey.set(state);
            outVal.set("T\t" + city + "\t" + temperature);
            context.write(outKey, outVal);
        }
    }

    public static class CapitalMapper extends Mapper<LongWritable, Text, Text, Text>
    { 
        private final Text outKey = new Text(); 
        private final Text outVal = new Text(); 

        @Override 
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        { 
            String line = value.toString();
            if (line.trim().isEmpty()) return;

            String[] csvRows = line.split(",", -1);
            if (csvRows.length <= CAP_CAPITAL) return;

            String state = csvRows[CAP_STATE], capital = csvRows[CAP_CAPITAL];
            if (state.equals("State") || state.isEmpty() || capital.isEmpty()) return;

            if (csvRows.length > CAP_TYPE && (csvRows[CAP_TYPE].trim()).equals("state_capital")) return;

            outKey.set(state);
            outVal.set("C\t" + capital);
            context.write(outKey, outVal);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text>
    { 
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override 
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        { 
            String capital = null; 
            Map<String, double[]> capitalPerCity = new HashMap<>();

            for (Text value : values) 
            { 
                String[] parts = value.toString().split("\t");

                if (parts[0].equals("C"))
                { 
                    capital = parts[1];
                } else if (parts[0].equals("T"))
                { 
                    double[] acc = capitalPerCity.get(parts[1]);
                    if (acc == null)
                    {
                        acc = new double[2];
                        capitalPerCity.put(parts[1], acc);
                    }
                    acc[0] += Double.parseDouble(parts[2]);
                    acc[1] += 1;
                }
            }

            if (capital == null) return;
            double[] acc = capitalPerCity.get(capital);
            if (acc == null || acc[1] == 0) return; 

            outKey.set(key.toString() + "\t" + capital);
            outVal.set(String.format(Locale.US, "%.2f", acc[0] / acc[1]));
            context.write(outKey, outVal);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    { 
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Q3 <in> <out>");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];

        Job job = Job.getInstance(config, "Q3");
        job.setJarByClass(Q3.class);

        MultipleInputs.addInputPath(job, new Path(in, "city_temperature.csv"), TextInputFormat.class, TemperatureMapper.class);
        MultipleInputs.addInputPath(job, new Path(in, "state-capitals.csv"), TextInputFormat.class, CapitalMapper.class);

        job.setReducerClass(JoinReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
