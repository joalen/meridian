package com.joalen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.naming.Context;

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

public class Q2 {
    private static final int STATE = 2;
    private static final int CITY = 3;
    private static final int TEMP = 7;
    private static final double INVALID = -99.0;

    public static class CapitalJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, String> capitals = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outVal = new Text();
    
        private static String clean(String s) {
            return s.trim().replace("\"", "");
        }
    
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
    
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("state-capitals.csv was not added to cache");
            }
    
            String localName = new Path(cacheFiles[0].getPath()).getName();
    
            try (BufferedReader br = new BufferedReader(new FileReader(localName))) {
                int stateCol = 0, capitalCol = 1;
                boolean first = true;
                String line;
    
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] f = line.split(",", -1);
    
                    if (first) {
                        first = false;
                        boolean isHeader = false;
    
                        for (int i = 0; i < f.length; i++) {
                            String h = clean(f[i]).toLowerCase(Locale.US);
                            if (h.equals("state")) { stateCol = i; isHeader = true; }
                            else if (h.equals("capital")) { capitalCol = i; isHeader = true; }
                        }
                        if (isHeader) continue;
                    }
    
                    if (f.length <= Math.max(stateCol, capitalCol)) continue;
                    capitals.put(clean(f[stateCol]), clean(f[capitalCol]));
                }
            }
        }
    
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) return;
    
            String[] row = line.split(",", -1);
            if (row.length <= TEMP) return;
    
            String state = row[STATE].trim(), city = row[CITY].trim();
            if (state.equals("State")) return; // header
    
            String capital = capitals.get(state);
            if (capital == null || !capital.equals(city)) return;
    
            double temperature;
            try {
                temperature = Double.parseDouble(row[TEMP].trim());
            } catch (NumberFormatException nfe) {
                return;
            }
    
            if (temperature == INVALID) return;
    
            outKey.set(state + "\t" + city);
            outVal.set(temperature + ",1");
            context.write(outKey, outVal);
        }
    }

    public static class SumCountCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }

            outVal.set(sum + "," + count);
            context.write(key, outVal);
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                sum += Double.parseDouble(parts[0]);
                count += Long.parseLong(parts[1]);
            }

            if (count == 0)
                return;
            outVal.set(String.format(Locale.US, "%.2f", sum / count));
            context.write(key, outVal);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Q2 <in> <out>");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];

        Job job = Job.getInstance(config, "Q2");
        job.setJarByClass(Q2.class);

        job.setMapperClass(CapitalJoinMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(in, "city_temperature.csv"));
        FileOutputFormat.setOutputPath(job, new Path(out));
        
        job.addCacheFile(new URI(new Path(in, "state-capitals.csv").toString() + "#state-capitals.csv"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
