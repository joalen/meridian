package com.joalen.chain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * Writes the partition-boundary file that TotalOrderPartitioner reads.
 *
 * What the file is: a SequenceFile of (ChainKey, NullWritable) holding
 * numReducers-1 keys in ascending sort order. Reducer i receives every key k
 * with  boundary[i-1] <= k < boundary[i]  (comparing with ChainSortComparator),
 * so reducer 0 gets the oldest chains and the last reducer the newest.
 *
 * Why not Hadoop's InputSampler: InputSampler samples the keys of the INPUT
 * format (here LongWritable byte offsets from TextInputFormat), but the file
 * must hold MAP-OUTPUT keys (ChainKey). Bridging that normally takes an extra
 * job that rewrites the input as a SequenceFile of ChainKeys. The input here is
 * small, so the driver samples the text file directly instead (reservoir
 * sampling, one pass, fixed seed). On a truly large input you would sample
 * splits rather than scan everything.
 *
 * Two rules that make the boundaries safe:
 *
 *   1. Boundaries are CHAIN STARTS (ChainKey.chainStart), never real record
 *      keys. A real key such as (origin, root, position 5, ...) as a boundary
 *      would put positions 0-4 of that chain on one reducer and 5+ on the next,
 *      and the chain would be resolved in two broken halves. A chain start sorts
 *      before every record of its chain, so each chain lies wholly on one side.
 *
 *   2. Boundaries are strictly increasing. TotalOrderPartitioner rejects
 *      duplicates ("Split points are out of order"), and many sampled records
 *      belong to the same big chain, so duplicates are skipped.
 *
 * Sampling RECORDS (not chains) is deliberate: reducer work is proportional to
 * records, so equal-record ranges give balanced reducers; a chain with 500
 * records is 500 times as likely to be sampled as a single-object chain.
 */
public final class ChainSampler {

    private static final int FIELDS = 10;

    /** @return the number of boundaries written (numReducers - 1) */
    public static int writePartitionFile(Configuration conf, Path input, Path partitionFile,
                                         int numReducers, int sampleSize, long seed) throws IOException {
        if (numReducers < 2) {
            throw new IllegalArgumentException("a partition file needs at least 2 reducers");
        }

        List<ChainKey> samples = reservoir(conf, input, sampleSize, seed);
        if (samples.isEmpty()) {
            throw new IOException("no input records found under " + input);
        }

        ChainSortComparator cmp = new ChainSortComparator();
        Collections.sort(samples, new java.util.Comparator<ChainKey>() {
            @Override
            public int compare(ChainKey a, ChainKey b) {
                return cmp.compare(a, b);
            }
        });

        // Evenly spaced quantiles of the sorted sample, skipping duplicates.
        List<ChainKey> boundaries = new ArrayList<>();
        float step = samples.size() / (float) numReducers;
        int last = -1;
        for (int i = 1; i < numReducers; i++) {
            int k = Math.round(step * i);
            while (k < samples.size() && (last >= 0 && cmp.compare(samples.get(last), samples.get(k)) == 0)) {
                k++;
            }
            if (k >= samples.size()) {
                throw new IOException("sample has too few distinct chains (" + boundaries.size()
                        + " boundaries found, " + (numReducers - 1) + " needed); use fewer reducers");
            }
            boundaries.add(samples.get(k));
            last = k;
        }

        FileSystem fs = partitionFile.getFileSystem(conf);
        fs.delete(partitionFile, false);
        try (SequenceFile.Writer w = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(partitionFile),
                SequenceFile.Writer.keyClass(ChainKey.class),
                SequenceFile.Writer.valueClass(NullWritable.class))) {
            for (ChainKey b : boundaries) {
                w.append(b, NullWritable.get());
            }
        }
        return boundaries.size();
    }

    /** Reservoir-samples `size` records; each is reduced to its chain's start key. */
    private static List<ChainKey> reservoir(Configuration conf, Path input, int size, long seed)
            throws IOException {
        FileSystem fs = input.getFileSystem(conf);
        List<Path> files = new ArrayList<>();
        FileStatus st = fs.getFileStatus(input);
        if (st.isFile()) {
            files.add(input);
        } else {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(input, true);
            while (it.hasNext()) {
                LocatedFileStatus f = it.next();
                String name = f.getPath().getName();
                if (!name.startsWith("_") && !name.startsWith(".")) {
                    files.add(f.getPath());
                }
            }
            Collections.sort(files);
        }

        Random rnd = new Random(seed);
        List<ChainKey> res = new ArrayList<>(size);
        long seen = 0;
        for (Path p : files) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isEmpty()) continue;
                    String[] f = line.split("\t", -1);
                    if (f.length != FIELDS) {
                        throw new IOException("total-order mode needs " + FIELDS
                                + "-column input (run ChainOriginAnnotator); got " + f.length
                                + " columns in " + p);
                    }
                    ChainKey k = ChainKey.chainStart(Long.parseLong(f[9]), f[0]);
                    seen++;
                    if (res.size() < size) {
                        res.add(k);
                    } else {
                        long j = (long) (rnd.nextDouble() * seen);
                        if (j < size) {
                            res.set((int) j, k);
                        }
                    }
                }
            }
        }
        return res;
    }

    private ChainSampler() {}
}
