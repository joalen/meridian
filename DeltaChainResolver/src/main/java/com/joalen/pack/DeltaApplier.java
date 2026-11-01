package com.joalen.pack;

import java.io.ByteArrayOutputStream;

/**
 * Applies one git delta to a resolved base object.
 *
 * Delta stream (after zlib inflation):
 *   varint baseSize      must equal base.length
 *   varint resultSize    length of the object this delta produces
 *   then instructions until the stream ends:
 *
 *   COPY   (op byte has bit 7 set): copy a range out of the base.
 *          Bits 0-3 of op say which of up to 4 offset bytes follow (little-endian),
 *          bits 4-6 say which of up to 3 size bytes follow. Absent bytes are 0.
 *          A decoded size of 0 means 0x10000 (git's historical encoding quirk).
 *   INSERT (op byte 1..127): the next `op` bytes are literal data to append.
 *   op == 0 is reserved and invalid.
 *
 * The size varint here (7 bits per byte, bit 7 = "more", little-endian) is the
 * same style as the pack object header's continuation bytes but has no type nibble.
 */
public final class DeltaApplier {

    public static byte[] apply(byte[] base, byte[] delta) {
        int[] pos = {0};

        long baseSize = readSize(delta, pos);
        long resultSize = readSize(delta, pos);
        if (baseSize != base.length) {
            throw new IllegalStateException("delta expects base of " + baseSize
                    + " bytes but base has " + base.length);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream((int) resultSize);

        while (pos[0] < delta.length) {
            int op = delta[pos[0]++] & 0xff;

            if ((op & 0x80) != 0) {
                long copyOffset = 0;
                long copySize = 0;
                if ((op & 0x01) != 0) copyOffset |= (long) next(delta, pos);
                if ((op & 0x02) != 0) copyOffset |= (long) next(delta, pos) << 8;
                if ((op & 0x04) != 0) copyOffset |= (long) next(delta, pos) << 16;
                if ((op & 0x08) != 0) copyOffset |= (long) next(delta, pos) << 24;
                if ((op & 0x10) != 0) copySize |= (long) next(delta, pos);
                if ((op & 0x20) != 0) copySize |= (long) next(delta, pos) << 8;
                if ((op & 0x40) != 0) copySize |= (long) next(delta, pos) << 16;
                if (copySize == 0) copySize = 0x10000;

                if (copyOffset + copySize > base.length) {
                    throw new IllegalStateException("copy out of range: offset " + copyOffset
                            + " size " + copySize + " base " + base.length);
                }
                out.write(base, (int) copyOffset, (int) copySize);
            } else if (op != 0) {
                if (pos[0] + op > delta.length) {
                    throw new IllegalStateException("insert runs past end of delta");
                }
                out.write(delta, pos[0], op);
                pos[0] += op;
            } else {
                throw new IllegalStateException("reserved delta opcode 0");
            }
        }

        byte[] result = out.toByteArray();
        if (result.length != resultSize) {
            throw new IllegalStateException("delta produced " + result.length
                    + " bytes but declared " + resultSize);
        }
        return result;
    }

    private static int next(byte[] data, int[] pos) {
        if (pos[0] >= data.length) {
            throw new IllegalStateException("truncated delta");
        }
        return data[pos[0]++] & 0xff;
    }

    private static long readSize(byte[] data, int[] pos) {
        long result = 0;
        int shift = 0;
        int c;
        do {
            c = next(data, pos);
            result |= (long) (c & 0x7f) << shift;
            shift += 7;
        } while ((c & 0x80) != 0);
        return result;
    }

    private DeltaApplier() {}
}
