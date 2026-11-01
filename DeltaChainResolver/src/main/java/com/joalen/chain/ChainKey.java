package com.joalen.chain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key for the secondary sort, five fields in sort order:
 *
 *   chainOrigin      when the chain's OLDEST object entered history (epoch seconds)   ascending
 *   chainRootId      which chain (the sha of its non-delta root)                      ascending
 *   chainPosition    where in the chain's dependency order (0 = root, then depth)     ascending
 *   objectSize       pre-pass "size" column: patch length (delta) / content length    DESCENDING
 *   objectTimestamp  when this object entered history (epoch seconds)                 ascending
 *
 * Stage 4 puts chainOrigin FIRST. It is a property of the whole chain (every
 * record of a chain carries the same value), so it orders chains against each
 * other without ever splitting one chain. That is what lets a range partitioner
 * hand reducer 0 the oldest chains, reducer 1 the next, and so on.
 *
 * Who reads what:
 *
 *   ChainPartitioner         -> chainRootId only   (hash mode: which reducer)
 *   TotalOrderPartitioner    -> the whole key via the sort comparator (range mode)
 *   ChainGroupingComparator  -> chainRootId only   (which keys share one reduce() call)
 *   ChainSortComparator      -> all five fields, mixed directions
 *
 * compareTo implements the same ordering as ChainSortComparator.
 */
public final class ChainKey implements WritableComparable<ChainKey> {
    private long chainOrigin;
    private final Text chainRootId = new Text();
    private long chainPosition;
    private long objectSize;
    private long objectTimestamp;

    public ChainKey() {}

    public ChainKey(String chainRootId, long chainPosition) {
        set(0L, chainRootId, chainPosition, 0L, 0L);
    }

    public ChainKey(String chainRootId, long chainPosition, long objectTimestamp) {
        set(0L, chainRootId, chainPosition, objectTimestamp, 0L);
    }

    public ChainKey(String chainRootId, long chainPosition, long objectTimestamp, long objectSize) {
        set(0L, chainRootId, chainPosition, objectTimestamp, objectSize);
    }

    public ChainKey(long chainOrigin, String chainRootId, long chainPosition,
                    long objectTimestamp, long objectSize) {
        set(chainOrigin, chainRootId, chainPosition, objectTimestamp, objectSize);
    }

    /** Lets the mapper reuse one instance instead of allocating per record. */
    public void set(long chainOrigin, String chainRootId, long chainPosition,
                    long objectTimestamp, long objectSize) {
        this.chainOrigin = chainOrigin;
        this.chainRootId.set(chainRootId);
        this.chainPosition = chainPosition;
        this.objectTimestamp = objectTimestamp;
        this.objectSize = objectSize;
    }

    /** No chain origin (input without the 10th column): origin 0. */
    public void set(String chainRootId, long chainPosition, long objectTimestamp, long objectSize) {
        set(0L, chainRootId, chainPosition, objectTimestamp, objectSize);
    }

    public void set(String chainRootId, long chainPosition, long objectTimestamp) {
        set(0L, chainRootId, chainPosition, objectTimestamp, 0L);
    }

    public void set(String chainRootId, long chainPosition) {
        set(0L, chainRootId, chainPosition, 0L, 0L);
    }

    /**
     * The smallest possible key of a chain: sorts before every real record of
     * (origin, root) and after every record of any earlier chain. Partition
     * boundaries are built from these so a boundary can never fall INSIDE a chain.
     */
    public static ChainKey chainStart(long chainOrigin, String chainRootId) {
        // size is DESCENDING, so the smallest key has the LARGEST size.
        return new ChainKey(chainOrigin, chainRootId, Long.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public long getChainOrigin() {
        return chainOrigin;
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
        out.writeLong(chainOrigin);
        chainRootId.write(out);
        out.writeLong(chainPosition);
        out.writeLong(objectSize);
        out.writeLong(objectTimestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        chainOrigin = in.readLong();
        chainRootId.readFields(in);
        chainPosition = in.readLong();
        objectSize = in.readLong();
        objectTimestamp = in.readLong();
    }

    /**
     * Full ordering: chainOrigin asc, chainRootId asc, chainPosition asc,
     * objectSize DESC, objectTimestamp asc. The size term swaps the operands;
     * subtracting (other - this) is the classic bug (it can overflow, and
     * chainStart() really does use Long.MAX_VALUE).
     */
    @Override
    public int compareTo(ChainKey other) {
        int c = Long.compare(chainOrigin, other.chainOrigin);
        if (c != 0) {
            return c;
        }
        c = chainRootId.compareTo(other.chainRootId);
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

    /** Covers all five fields, consistent with compareTo/equals. */
    @Override
    public int hashCode() {
        int h = Long.hashCode(chainOrigin);
        h = h * 163 + chainRootId.hashCode();
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
        return chainOrigin == other.chainOrigin
                && chainPosition == other.chainPosition
                && objectSize == other.objectSize
                && objectTimestamp == other.objectTimestamp
                && chainRootId.equals(other.chainRootId);
    }

    @Override
    public String toString() {
        return chainOrigin + "@" + chainRootId + "@" + chainPosition + "@" + objectSize + "@" + objectTimestamp;
    }
}
