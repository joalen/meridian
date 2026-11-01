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
import com.joalen.chain.DeltaChainMapper;
import com.joalen.chain.DeltaChainReducer;

/**
 * Stage 1: baseline secondary sort over git delta-chain topology.
 *
 * Input:  TopologyPrePass output (rootSha, position, sha, baseSha, type, size, offset per line)
 * Output: one diagnostic line per chain root (see DeltaChainReducer)
 *
 * Usage: hadoop jar DeltaChainResolver.jar <in> <out> [numReducers=4]
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

        Job job = Job.getInstance(config, "DeltaChainResolver - Stage 1");
        job.setJarByClass(DeltaChainResolver.class);

        job.setMapperClass(DeltaChainMapper.class);
        job.setReducerClass(DeltaChainReducer.class);
        job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(ChainKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Three independent knobs; setting one does not imply the others.
        job.setPartitionerClass(ChainPartitioner.class);               // which reducer
        job.setGroupingComparatorClass(ChainGroupingComparator.class); // which keys share a reduce() call
        // Sort order: ChainKey.compareTo (natural ordering) is used by default.

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}