package com.joalen.chain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key for the secondary sort, three fields:
 *
 *   chainRootId      which chain (the sha of its non-delta root)
 *   chainPosition    where in the chain's dependency order (0 = root, then depth)
 *   objectTimestamp  when the object entered history (epoch seconds; 0 = unknown)
 *
 * Three different jobs read different parts of this key, and keeping them
 * separate is the point of Stage 2:
 *
 *   ChainPartitioner         -> chainRootId only            (which reducer)
 *   ChainGroupingComparator  -> chainRootId only            (which keys share one reduce() call)
 *   ChainSortComparator      -> all three fields, in order  (the order values arrive in)
 *
 * compareTo is the same full ordering as ChainSortComparator, so the key stays
 * correct even if the sort comparator is not set on the Job.
 */
public final class ChainKey implements WritableComparable<ChainKey> {
    private final Text chainRootId = new Text();
    private long chainPosition;
    private long objectTimestamp;

    public ChainKey() {}

    public ChainKey(String chainRootId, long chainPosition) {
        set(chainRootId, chainPosition, 0L);
    }

    public ChainKey(String chainRootId, long chainPosition, long objectTimestamp) {
        set(chainRootId, chainPosition, objectTimestamp);
    }

    /** Lets the mapper reuse one instance instead of allocating per record. */
    public void set(String chainRootId, long chainPosition, long objectTimestamp) {
        this.chainRootId.set(chainRootId);
        this.chainPosition = chainPosition;
        this.objectTimestamp = objectTimestamp;
    }

    /** Position-only form (topology input without timestamps): timestamp 0. */
    public void set(String chainRootId, long chainPosition) {
        set(chainRootId, chainPosition, 0L);
    }

    public String getChainRootId() {
        return chainRootId.toString();
    }

    public long getChainPosition() {
        return chainPosition;
    }

    public long getObjectTimestamp() {
        return objectTimestamp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        chainRootId.write(out);
        out.writeLong(chainPosition);
        out.writeLong(objectTimestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        chainRootId.readFields(in);
        chainPosition = in.readLong();
        objectTimestamp = in.readLong();
    }

    /** Full ordering: chainRootId, then chainPosition, then objectTimestamp, all ascending. */
    @Override
    public int compareTo(ChainKey other) {
        int c = chainRootId.compareTo(other.chainRootId);
        if (c != 0) {
            return c;
        }
        c = Long.compare(chainPosition, other.chainPosition);
        if (c != 0) {
            return c;
        }
        return Long.compare(objectTimestamp, other.objectTimestamp);
    }

    /** Covers all three fields, consistent with compareTo/equals. */
    @Override
    public int hashCode() {
        int h = chainRootId.hashCode();
        h = h * 163 + Long.hashCode(chainPosition);
        h = h * 163 + Long.hashCode(objectTimestamp);
        return h;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChainKey)) return false;
        ChainKey other = (ChainKey) o;
        return chainPosition == other.chainPosition
                && objectTimestamp == other.objectTimestamp
                && chainRootId.equals(other.chainRootId);
    }

    @Override
    public String toString() {
        return chainRootId + "@" + chainPosition + "@" + objectTimestamp;
    }
}
