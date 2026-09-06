package com.joalen.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * Mapper stage for MapReduce that helps build an inverted index from Q2 dataset
 */
public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
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
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    { 
        String[] fields = value.toString().split(",", -1); // no empty state columns 

        if (fields.length < 10)
        { 
            return;
        }

        int lineNumberFromDataset; 
        try { 
            lineNumberFromDataset = Integer.parseInt(fields[0].trim());
        } catch (NumberFormatException nfe)
        { 
            return;
        }

        lineNumber.set()lineNumberFromDataset;

        for (int index : INDEXED_FIELDS)
        { 
            String fieldValue = fields[index].trim().toLowerCase(); 

            if (fieldValue.isEmpty())
            { 
                continue;
            }
            
            String tokens = NON_LETTER.split(fieldValue);

            for (String token : tokens)
            { 
                if (!token.isEmpty())
                { 
                    word.set(token);
                    context.write(word, lineNumber);
                }
            }
        }
    }
}
