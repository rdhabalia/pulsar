package io.streamlake;

import java.nio.charset.StandardCharsets;

/**
 * Column scalar types supported by the Streaming Lake columnar page format.
 *
 * <p>Each type knows how to:
 * <ul>
 *   <li>encode/decode a value into the columnar payload (row reconstruction), and</li>
 *   <li>produce an <b>order-preserving</b> byte encoding for the range index.</li>
 * </ul>
 *
 * The order-preserving encoding is the key to keeping the bookie schema-agnostic:
 * the bookie compares page ranges against predicate ranges using pure lexicographic
 * {@code byte[]} comparison and never needs to know the column's type or name.
 */
public enum ColumnType {
    INT((byte) 0),
    LONG((byte) 1),
    STRING((byte) 4);

    public final byte tag;

    ColumnType(byte tag) {
        this.tag = tag;
    }

    public static ColumnType fromTag(byte tag) {
        for (ColumnType t : values()) {
            if (t.tag == tag) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown column type tag: " + tag);
    }

    /**
     * Order-preserving byte encoding: lexicographic comparison of the returned
     * bytes is consistent with the natural ordering of the values. This is what
     * lets the bookie prune pages by comparing bytes only.
     */
    public byte[] encodeOrderPreserving(Object value) {
        switch (this) {
            case INT: {
                int v = ((Number) value).intValue();
                int u = v ^ 0x80000000;               // flip sign bit -> unsigned order == signed order
                return new byte[]{
                        (byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u
                };
            }
            case LONG: {
                long v = ((Number) value).longValue();
                long u = v ^ 0x8000000000000000L;
                byte[] b = new byte[8];
                for (int i = 7; i >= 0; i--) {
                    b[i] = (byte) u;
                    u >>>= 8;
                }
                return b;
            }
            case STRING:
                return ((String) value).getBytes(StandardCharsets.UTF_8);
            default:
                throw new IllegalStateException();
        }
    }
}
