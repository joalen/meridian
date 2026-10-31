package com.joalen.chain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key for the secondary sort: which chain an object belongs to
 * (the sha of its non-delta root), and where in that chain's dependency
 * order it sits (0 = the root itself, increasing with delta depth).
 *
 * Natural ordering (compareTo) is the SORT order: root first, then position
 * ascending. That is deliberately different from the GROUPING order
 * (ChainGroupingComparator: root only) and the PARTITIONING rule
 * (ChainPartitioner: root only).
 */
public final class ChainKey implements WritableComparable<ChainKey> {
    private final Text chainRootId = new Text();
    private long chainPosition;

    public ChainKey() {}

    public ChainKey(String chainRootId, long chainPosition) {
        set(chainRootId, chainPosition);
    }

    /** Lets the mapper reuse one instance instead of allocating per record. */
    public void set(String chainRootId, long chainPosition) {
        this.chainRootId.set(chainRootId);
        this.chainPosition = chainPosition;
    }

    public String getChainRootId() {
        return chainRootId.toString();
    }

    public long getChainPosition() {
        return chainPosition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        chainRootId.write(out);
        out.writeLong(chainPosition);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        chainRootId.readFields(in);
        chainPosition = in.readLong();
    }

    /** Full-key ordering: chainRootId, then chainPosition ascending. */
    @Override
    public int compareTo(ChainKey other) {
        int c = chainRootId.compareTo(other.chainRootId);
        if (c != 0) {
            return c;
        }
        return Long.compare(chainPosition, other.chainPosition);
    }

    /** Covers both fields, consistent with compareTo/equals. */
    @Override
    public int hashCode() {
        return chainRootId.hashCode() * 163 + Long.hashCode(chainPosition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChainKey)) return false;
        ChainKey other = (ChainKey) o;
        return chainPosition == other.chainPosition && chainRootId.equals(other.chainRootId);
    }

    @Override
    public String toString() {
        return chainRootId + "@" + chainPosition;
    }
}
