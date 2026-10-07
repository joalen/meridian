package com.joalen;

import java.io.IOException;

import javax.naming.Context;

import org.w3c.dom.Text;

public class Q3 
{
    // columns city_temperatures.csv
    private static final int STATE = 2;
    private static final int CITY = 3;
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

            outKey.set(state);
            outVal.set("T\t" + city + "\t" + temperature);
            context.write(outKey, outVal);
        }
    }
}
