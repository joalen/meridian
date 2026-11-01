package com.joalen.pack;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.InflaterInputStream;

/**
 * Reads ONE raw object from a git .pack file at a known byte offset.
 *
 * for a non-delta object that is the full object content; for a delta object it 
 * is the delta instruction stream (not yet applied to its base).
 *
 * Pack object layout at an offset:
 *   1. header varint: first byte = [continue:1][type:3][size low 4 bits],
 *      following bytes = [continue:1][next 7 size bits], little-endian groups
 *   2. OBJ_OFS_DELTA (6): a different varint giving a BACKWARD distance to the base
 *      OBJ_REF_DELTA (7): 20 raw bytes, the base object's SHA-1
 *   3. zlib-deflated data; `size` from the header is the INFLATED length
 */
public final class PackReader implements Closeable {

    public static final int OBJ_COMMIT = 1;
    public static final int OBJ_TREE = 2;
    public static final int OBJ_BLOB = 3;
    public static final int OBJ_TAG = 4;
    public static final int OBJ_OFS_DELTA = 6;
    public static final int OBJ_REF_DELTA = 7;

    /** One object exactly as stored in the pack. */
    public static final class RawObject {
        public final int type;          // one of the OBJ_* constants
        public final long baseOffset;   // pack offset of base; only for OBJ_OFS_DELTA, else -1
        public final byte[] baseSha1;   // 20 bytes; only for OBJ_REF_DELTA, else null
        public final byte[] data;       // inflated payload (full content, or delta stream)

        RawObject(int type, long baseOffset, byte[] baseSha1, byte[] data) {
            this.type = type;
            this.baseOffset = baseOffset;
            this.baseSha1 = baseSha1;
            this.data = data;
        }

        public boolean isDelta() {
            return type == OBJ_OFS_DELTA || type == OBJ_REF_DELTA;
        }
    }

    private final FileChannel channel;

    public PackReader(Path packFile) throws IOException {
        this.channel = FileChannel.open(packFile, StandardOpenOption.READ);
    }

    /** Reads the object that starts at `offset` in the pack. */
    public RawObject readAt(long offset) throws IOException {
        channel.position(offset);
        InputStream in = new BufferedInputStream(Channels.newInputStream(channel), 8192);

        /* 
            Header type and inflated size
        */
        int c = readByte(in);
        int type = (c >> 4) & 7;
        long size = c & 15;
        int shift = 4; // the first byte only carried 4 size bits
        while ((c & 0x80) != 0) {
            c = readByte(in);
            size |= (long) (c & 0x7f) << shift;
            shift += 7;
        }

        /* 
            Base reference
        */
        long baseOffset = -1;
        byte[] baseSha = null;
        if (type == OBJ_OFS_DELTA) {
            // Offset varint is NOT the same encoding as the size varint above:
            // big-endian groups, and every continuation adds 1 before shifting.
            c = readByte(in);
            long rel = c & 0x7f;
            while ((c & 0x80) != 0) {
                c = readByte(in);
                rel = ((rel + 1) << 7) | (c & 0x7f);
            }
            baseOffset = offset - rel;
        } else if (type == OBJ_REF_DELTA) {
            baseSha = new byte[20];
            readFully(in, baseSha, 0, 20);
        } else if (type < OBJ_COMMIT || type > OBJ_TAG) {
            throw new IOException("unknown object type " + type + " at offset " + offset);
        }
        
        /* 
            Zlib payload
        */
        if (size > Integer.MAX_VALUE) {
            throw new IOException("object too large for in-memory read: " + size);
        }
        byte[] data = new byte[(int) size];
        InflaterInputStream inflater = new InflaterInputStream(in);
        readFully(inflater, data, 0, data.length);

        return new RawObject(type, baseOffset, baseSha, data);
    }

    private static int readByte(InputStream in) throws IOException {
        int b = in.read();
        if (b < 0) throw new EOFException("unexpected end of pack");
        return b;
    }

    private static void readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int done = 0;
        while (done < len) {
            int n = in.read(buf, off + done, len - done);
            if (n < 0) throw new EOFException("unexpected end of data after " + done + " of " + len + " bytes");
            done += n;
        }
    }

    /** Name used in the git object header that gets hashed ("blob", "tree", ...). */
    public static String typeName(int type) {
        switch (type) {
            case OBJ_COMMIT: return "commit";
            case OBJ_TREE:   return "tree";
            case OBJ_BLOB:   return "blob";
            case OBJ_TAG:    return "tag";
            default: throw new IllegalArgumentException("not a base object type: " + type);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
