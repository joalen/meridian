package com.joalen.chain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key for the secondary sort, four fields:
 *
 *   chainRootId      which chain (the sha of its non-delta root)
 *   chainPosition    where in the chain's dependency order (0 = root, then depth)   ascending
 *   objectSize       the pre-pass "size" column: patch length for a delta, content
 *                    length for a root                                              DESCENDING
 *   objectTimestamp  when the object entered history (epoch seconds; 0 = unknown)   ascending
 *
 * Three different jobs read different parts of this key:
 *
 *   ChainPartitioner         -> chainRootId only            (which reducer)
 *   ChainGroupingComparator  -> chainRootId only            (which keys share one reduce() call)
 *   ChainSortComparator      -> all four fields, mixed directions
 *
 * Stage 3's point: the sort is NOT "everything ascending". Position runs up,
 * size runs down. compareTo implements the same ordering as ChainSortComparator,
 * so the key stays correct even if the sort comparator is not set on the Job.
 */
public final class ChainKey implements WritableComparable<ChainKey> {
    private final Text chainRootId = new Text();
    private long chainPosition;
    private long objectSize;
    private long objectTimestamp;

    public ChainKey() {}

    public ChainKey(String chainRootId, long chainPosition) {
        set(chainRootId, chainPosition, 0L);
    }

    public ChainKey(String chainRootId, long chainPosition, long objectTimestamp) {
        set(chainRootId, chainPosition, objectTimestamp);
    }

    public ChainKey(String chainRootId, long chainPosition, long objectTimestamp, long objectSize) {
        set(chainRootId, chainPosition, objectTimestamp, objectSize);
    }

    /** Lets the mapper reuse one instance instead of allocating per record. */
    public void set(String chainRootId, long chainPosition, long objectTimestamp, long objectSize) {
        this.chainRootId.set(chainRootId);
        this.chainPosition = chainPosition;
        this.objectTimestamp = objectTimestamp;
        this.objectSize = objectSize;
    }

    /** No size known: size 0 (every record equal on size, so size-descending changes nothing). */
    public void set(String chainRootId, long chainPosition, long objectTimestamp) {
        set(chainRootId, chainPosition, objectTimestamp, 0L);
    }

    /** Position-only form (topology input without timestamps): timestamp 0, size 0. */
    public void set(String chainRootId, long chainPosition) {
        set(chainRootId, chainPosition, 0L, 0L);
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

    public long getObjectSize() {
        return objectSize;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        chainRootId.write(out);
        out.writeLong(chainPosition);
        out.writeLong(objectSize);
        out.writeLong(objectTimestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        chainRootId.readFields(in);
        chainPosition = in.readLong();
        objectSize = in.readLong();
        objectTimestamp = in.readLong();
    }

    /**
     * Full ordering: chainRootId ascending, chainPosition ascending,
     * objectSize DESCENDING, objectTimestamp ascending.
     * The size term swaps the operands (Long.compare(other, this)); subtracting
     * (other - this) is the classic bug: it can overflow for large values.
     */
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
        c = Long.compare(other.objectSize, objectSize); // descending
        if (c != 0) {
            return c;
        }
        return Long.compare(objectTimestamp, other.objectTimestamp);
    }

    /** Covers all four fields, consistent with compareTo/equals. */
    @Override
    public int hashCode() {
        int h = chainRootId.hashCode();
        h = h * 163 + Long.hashCode(chainPosition);
        h = h * 163 + Long.hashCode(objectSize);
        h = h * 163 + Long.hashCode(objectTimestamp);
        return h;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChainKey)) return false;
        ChainKey other = (ChainKey) o;
        return chainPosition == other.chainPosition
                && objectSize == other.objectSize
                && objectTimestamp == other.objectTimestamp
                && chainRootId.equals(other.chainRootId);
    }

    @Override
    public String toString() {
        return chainRootId + "@" + chainPosition + "@" + objectSize + "@" + objectTimestamp;
    }
}
