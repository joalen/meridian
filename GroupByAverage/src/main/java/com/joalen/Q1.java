package com.joalen;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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
                String[] temperaturePart = value.toString().split(",");
                sum += Double.parseDouble(temperaturePart[0]);
                count += Long.parseLong(temperaturePart[1]);
            }

            outVal.set(sum + "," + count);
            context.write(key, outVal);
        }
    }

    public static void main( String[] args )
    {
        
    }
}
