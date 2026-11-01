package com.joalen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.joalen.chain.ChainGroupingComparator;
import com.joalen.chain.ChainKey;
import com.joalen.chain.ChainPartitioner;
import com.joalen.chain.ChainSortComparator;
import com.joalen.chain.DeltaChainMapper;
import com.joalen.chain.DeltaChainReducer;

/**
 * Stage 3: mixed-direction sort (root asc, position asc, size DESC, timestamp asc) over git delta chains.
 *
 * Input:  TimestampAnnotator output (9 columns); 7/8-column TSVs still work (timestamp 0)
 * Output: one diagnostic line per chain root (see DeltaChainReducer)
 *
 * Usage: hadoop jar DeltaChainResolver.jar <in> <out> [numReducers=4]
 *
 * Several reducers are the default on purpose: with a single reducer a
 * broken partitioner is invisible.
 */
public class DeltaChainResolver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        if (otherArgs.length < 2 || otherArgs.length > 3) {
            System.err.println("Usage: DeltaChainResolver <in> <out> [numReducers]");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];
        int numReducers = otherArgs.length == 3 ? Integer.parseInt(otherArgs[2]) : 4;

        Job job = Job.getInstance(config, "DeltaChainResolver - Stage 3");
        job.setJarByClass(DeltaChainResolver.class);

        job.setMapperClass(DeltaChainMapper.class);
        job.setReducerClass(DeltaChainReducer.class);
        job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(ChainKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Three independent knobs. Each is a separate setter; setting one
        // does not imply the others.
        job.setPartitionerClass(ChainPartitioner.class);               // which reducer
        job.setGroupingComparatorClass(ChainGroupingComparator.class); // which keys share a reduce() call
        job.setSortComparatorClass(ChainSortComparator.class);         // order: root asc, position asc, size DESC, timestamp asc

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
