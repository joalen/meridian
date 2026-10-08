package com.joalen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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

public class MapSideJoin {
    private static final int STATE = 2;
    private static final int CITY = 3;
    private static final int TEMP = 7;
    private static final double INVALID = -99.0;

    /** 
     * Performs map-side join between temperature data and state-capitals from the state-capitals.csv 
     * (converted into a hashmap), where we emit readings only for each state's capital city
     */
    public static class CapitalJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, String> capitals = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outVal = new Text();
    
        /**
         * Loads in the distributed cache's copy of state-capital.csv and generates a 
         * hashmap of states -> capital to act as a fast lookup table
         * 
         * @param context used for accessing the distributed cache files
         * @throws IOException if no cache file was provided or it cannot be read
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
    
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("dataset2.csv was not added to cache");
            }
    
            String localName = new Path(cacheFiles[0].getPath()).getName();
    
            try (BufferedReader br = new BufferedReader(new FileReader(localName))) {
                int stateCol = 0, capitalCol = 1;
                boolean first = true;
                String line;
    
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] parts = line.split(",", -1);
    
                    if (first) {
                        first = false;
                        boolean isHeader = false;
    
                        for (int i = 0; i < parts.length; i++) {
                            String h = (parts[i].trim()).toLowerCase(Locale.US);
                            if (h.equals("state")) { stateCol = i; isHeader = true; }
                            else if (h.equals("capital")) { capitalCol = i; isHeader = true; }
                        }
                        if (isHeader) continue;
                    }
    
                    if (parts.length <= Math.max(stateCol, capitalCol)) continue;
                    capitals.put(parts[stateCol].trim(), parts[capitalCol].trim());
                }
            }
        }
    
        /** 
         * Emits a (sum, count) pair from a temperature key in the city_temperature.csv provided 
         * we supply the state's capital city.
         * 
         * @param key byte offset of the line in the input file
         * @param value a line from the city_temperature.csv
         * @param context used for emitting (state/city, temperature/count) pair
         * 
         * @throws IOException system encounters an I/O error 
         * @throws InterruptedException Mapper task from MapReduce interrupted from system
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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

    /** 
     * Combiner that helps merge "sum,count" strings from mapper into a single, unified 
     * "sum,count" pair per key to alleviate shuffling traffic
     */
    public static class SumCountCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        /** 
         * Cumulative sum of partial sums and counts for one (state, city) key
         * 
         * @param key the {@code "state\tcity"} key representation
         * @param values "sum,count" strings from the mapper stage
         * @param context used for emitting combined "sum,count" values 
         * 
         * @throws IOException system encounters an I/O error 
         * @throws InterruptedException Combiner task from MapReduce interrupted from system
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

    /** 
     * Reducer that totals all sum,count values for a state,city pairing from combiner stage and 
     * transforms that into average temperature.
     */
    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outVal = new Text();

        /** 
         * Computes an average from sum,count value for a state,city pairing 
         * 
         * @param key the {@code "state\tcity"} key
         * @param values "sum,count" strings to aggregate
         * @param context used for emitting average temperature 
         * 
         * @throws IOException system encounters an I/O error 
         * @throws InterruptedException Reducer task from MapReduce interrupted from system
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
            System.err.println("Usage: MapSideJoin <in> <out>");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];

        Job job = Job.getInstance(config, "MapSideJoin");
        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(CapitalJoinMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(in, "dataset.csv"));
        FileOutputFormat.setOutputPath(job, new Path(out));
        
        job.addCacheFile(new URI(new Path(in, "dataset2.csv").toString() + "#dataset2.csv"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
