package org.deephacks.logbuffers.protobuf;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Varint32 {
    private int value;
    private ByteBuffer bytes;
    private int size;

    public Varint32(int value) {
        this.value = value;
    }

    public Varint32(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public ByteBuffer getByteBuffer() {
        return bytes;
    }

    public byte[] write() throws IOException {
        byte[] result = new byte[computeRawVarint32Size(value)];
        int pos = 0;
        while (true) {
            if ((value & ~0x7F) == 0) {
                result[pos++] = (byte) value;
                return result;
            } else {
                result[pos++] = (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    public int getSize() {
        if (size == 0) {
            size = computeRawVarint32Size(value);
        }
        return size;
    }

    private int computeRawVarint32Size(final int value) {
        if ((value & (0xffffffff << 7)) == 0)
            return 1;
        if ((value & (0xffffffff << 14)) == 0)
            return 2;
        if ((value & (0xffffffff << 21)) == 0)
            return 3;
        if ((value & (0xffffffff << 28)) == 0)
            return 4;
        return 5;
    }

    public int read() throws IOException {

        byte tmp = bytes.get();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = bytes.get()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = bytes.get()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = bytes.get()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = bytes.get()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (bytes.get() >= 0) {
                                return result;
                            }
                        }
                        throw new IllegalArgumentException();
                    }
                }
            }
        }
        return result;
    }
}