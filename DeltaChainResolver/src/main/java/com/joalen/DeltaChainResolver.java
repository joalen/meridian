package com.joalen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import com.joalen.chain.ChainGroupingComparator;
import com.joalen.chain.ChainKey;
import com.joalen.chain.ChainPartitioner;
import com.joalen.chain.ChainSampler;
import com.joalen.chain.ChainSortComparator;
import com.joalen.chain.DeltaChainMapper;
import com.joalen.chain.DeltaChainReducer;

/**
 * Stage 4: total order across reducers (range partitioning), on top of the
 * Stage 3 per-chain sort (position asc, size DESC).
 *
 * Input:  ChainOriginAnnotator output (10 columns) for total-order mode;
 *         7/8/9-column TSVs still work in hash mode (origin 0)
 * Output: one diagnostic line per chain. Read part-r-00000, part-r-00001, ...
 *         in order and the chains are in ascending chainOrigin order.
 *
 * Usage: hadoop jar DeltaChainResolver.jar <in> <out> [numReducers=4] [total|hash]
 *
 *   total (default)  TotalOrderPartitioner: reducer 0 gets the oldest chains,
 *                    reducer 1 the next range, ... boundaries come from a sampled
 *                    partition file written to <out>_partitions before the job runs.
 *   hash             Stage 1-3 behaviour: ChainPartitioner hashes the root, so each
 *                    reducer holds an arbitrary scatter of chains.
 *
 * Tunables (-D): chain.sample.size (default 2000), chain.sample.seed (default 42).
 *
 * Several reducers are the default on purpose: with a single reducer a
 * broken partitioner is invisible. With 1 reducer there is nothing to range-
 * partition, so the partitioner is left at Hadoop's default.
 */
public class DeltaChainResolver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        if (otherArgs.length < 2 || otherArgs.length > 4) {
            System.err.println("Usage: DeltaChainResolver <in> <out> [numReducers] [total|hash]");
            System.exit(2);
        }

        String in = otherArgs[0];
        String out = otherArgs[1];
        int numReducers = otherArgs.length >= 3 ? Integer.parseInt(otherArgs[2]) : 4;
        String mode = otherArgs.length == 4 ? otherArgs[3] : "total";
        if (!mode.equals("total") && !mode.equals("hash")) {
            System.err.println("mode must be 'total' or 'hash', got: " + mode);
            System.exit(2);
        }

        Job job = Job.getInstance(config, "DeltaChainResolver - Stage 4 (" + mode + ")");
        job.setJarByClass(DeltaChainResolver.class);

        job.setMapperClass(DeltaChainMapper.class);
        job.setReducerClass(DeltaChainReducer.class);
        job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(ChainKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Independent knobs. Each is a separate setter; setting one does not imply the others.
        job.setGroupingComparatorClass(ChainGroupingComparator.class); // which keys share a reduce() call
        job.setSortComparatorClass(ChainSortComparator.class);         // order: origin, root, position asc, size DESC, timestamp
        if (mode.equals("hash")) {
            job.setPartitionerClass(ChainPartitioner.class);           // which reducer: hash(root)
        } else if (numReducers > 1) {
            // Which reducer: the key's RANGE, using the sort comparator and a sampled boundary file.
            Path outPath = new Path(out);
            outPath = outPath.getFileSystem(config).makeQualified(outPath);
            Path partitionFile = new Path(outPath.getParent(), outPath.getName() + "_partitions");
            int sampleSize = config.getInt("chain.sample.size", 2000);
            long seed = config.getLong("chain.sample.seed", 42L);

            int written = ChainSampler.writePartitionFile(
                    config, new Path(in), partitionFile, numReducers, sampleSize, seed);
            System.out.println("partition file " + partitionFile + ": " + written + " boundaries");

            TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
            job.setPartitionerClass(TotalOrderPartitioner.class);
        }

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        boolean ok = job.waitForCompletion(true);

        // Print this project's counters on stdout so they are visible even when
        // the log output is long or filtered.
        if (ok) {
            for (CounterGroup group : job.getCounters()) {
                if (group.getName().startsWith("com.joalen")) {
                    for (Counter c : group) {
                        System.out.println(String.format("COUNTER %-24s %d", c.getName(), c.getValue()));
                    }
                }
            }
        }
        System.exit(ok ? 0 : 1);
    }
}
