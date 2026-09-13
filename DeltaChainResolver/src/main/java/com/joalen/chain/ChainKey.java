package com.joalen.chain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class ChainKey implements WritableComparable<ChainKey> {
    private Text chainRootId = new Text();
    private long chainPosition;

    public ChainKey() {}

    public ChainKey(String chainRootId, long chainPosition) {
        this.chainRootId.set(chainRootId);
        this.chainPosition = chainPosition;
    }

    public String getChainRootId() { return chainRootId.toString(); }
    public long getChainPosition() { return chainPosition; }

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

    @Override
    public int compareTo(ChainKey other) {
        int cmp = this.chainRootId.compareTo(other.chainRootId);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.chainPosition, other.chainPosition);
    }

    @Override
    public int hashCode() {
        return chainRootId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChainKey other = (ChainKey) o;
        return chainPosition == other.chainPosition
                && chainRootId.equals(other.chainRootId);
    }
}
