package io.streamlake;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JVM-native columnar page codec (the "Vortex fallback").
 *
 * <p>Binary layout:
 * <pre>
 *   magic(4)='SLP1' | version(2) | flags(1) | rowCount(4) | numCols(2)
 *   directory[numCols] : columnId(2) type(1) dataOffset(4) dataLength(4)
 *   data blocks (column-major):
 *      INT    : rowCount * 4   (big-endian)
 *      LONG   : rowCount * 8
 *      STRING : per row -> len(4) + utf8 bytes
 * </pre>
 * Column 0 (reserved) always carries the per-row eventTime as a LONG. flags bit0
 * marks the page as columnar/"vortex" encoded (the version + flag the design calls for).
 */
public final class ColumnarPageCodec implements PageCodec {

    private static final int MAGIC = 0x534C5031; // 'S','L','P','1'
    private static final short VERSION = 1;
    private static final byte FLAG_COLUMNAR = 0x1;

    private final SchemaDef schema;

    public ColumnarPageCodec(SchemaDef schema) {
        this.schema = schema;
    }

    /** Ordered list of (columnId, type) actually stored: eventTime first, then schema fields. */
    private List<short[]> layout() {
        // short[]{columnId, typeTag}
        List<short[]> cols = new ArrayList<>();
        cols.add(new short[]{EVENT_TIME_COLUMN, ColumnType.LONG.tag});
        for (Field f : schema.fields()) {
            cols.add(new short[]{f.columnId, f.type.tag});
        }
        return cols;
    }

    private ColumnType typeOf(short columnId) {
        if (columnId == EVENT_TIME_COLUMN) {
            return ColumnType.LONG;
        }
        return schema.byId(columnId).type;
    }

    private Object valueOf(Record r, short columnId) {
        return columnId == EVENT_TIME_COLUMN ? r.eventTime : r.get(columnId);
    }

    // ------------------------------------------------------------------ encode

    @Override
    public byte[] encode(List<Record> rows) {
        List<short[]> cols = layout();
        int numCols = cols.size();

        // Build each column block.
        byte[][] blocks = new byte[numCols][];
        for (int c = 0; c < numCols; c++) {
            short columnId = cols.get(c)[0];
            ColumnType type = ColumnType.fromTag((byte) cols.get(c)[1]);
            blocks[c] = encodeColumn(rows, columnId, type);
        }

        int headerSize = 4 + 2 + 1 + 4 + 2;
        int dirSize = numCols * (2 + 1 + 4 + 4);
        int dataStart = headerSize + dirSize;
        int total = dataStart;
        for (byte[] b : blocks) {
            total += b.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.putInt(MAGIC);
        buf.putShort(VERSION);
        buf.put(FLAG_COLUMNAR);
        buf.putInt(rows.size());
        buf.putShort((short) numCols);

        int offset = dataStart;
        for (int c = 0; c < numCols; c++) {
            buf.putShort(cols.get(c)[0]);          // columnId
            buf.put((byte) cols.get(c)[1]);        // type
            buf.putInt(offset);                    // dataOffset (absolute)
            buf.putInt(blocks[c].length);          // dataLength
            offset += blocks[c].length;
        }
        for (byte[] b : blocks) {
            buf.put(b);
        }
        return buf.array();
    }

    private byte[] encodeColumn(List<Record> rows, short columnId, ColumnType type) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Record r : rows) {
            Object v = valueOf(r, columnId);
            switch (type) {
                case INT:
                    writeInt(out, ((Number) v).intValue());
                    break;
                case LONG:
                    writeLong(out, ((Number) v).longValue());
                    break;
                case STRING:
                    byte[] s = ((String) v).getBytes(StandardCharsets.UTF_8);
                    writeInt(out, s.length);
                    out.write(s, 0, s.length);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        return out.toByteArray();
    }

    // ------------------------------------------------------------------ decode

    @Override
    public List<Record> decodeAll(byte[] page) {
        return decode(page, null);
    }

    @Override
    public List<Record> decodeMatching(byte[] page, Predicate predicate) {
        return decode(page, predicate);
    }

    private List<Record> decode(byte[] page, Predicate predicate) {
        Decoded d = new Decoded(page);
        List<Record> out = new ArrayList<>();
        RowView view = new RowView(d);
        for (int row = 0; row < d.rowCount; row++) {
            view.row = row;
            if (predicate != null && !predicate.eval(view)) {
                continue; // pushdown: skip non-matching rows without materializing
            }
            Record rec = new Record(view.getLong(EVENT_TIME_COLUMN));
            for (Field f : schema.fields()) {
                rec.put(f.columnId, view.get(f.columnId));
            }
            out.add(rec);
        }
        return out;
    }

    /** Parsed page directory + random-access string-offset cache. */
    private static final class Decoded {
        final ByteBuffer buf;
        final int rowCount;
        final Map<Short, int[]> dir = new HashMap<>();      // columnId -> {typeTag, dataOffset, dataLength}
        final Map<Short, int[]> strStarts = new HashMap<>(); // lazy: per-row byte start for STRING cols
        final Map<Short, int[]> strLens = new HashMap<>();

        Decoded(byte[] page) {
            this.buf = ByteBuffer.wrap(page);
            int magic = buf.getInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Not a Streaming Lake page");
            }
            buf.getShort();                 // version
            buf.get();                      // flags
            this.rowCount = buf.getInt();
            int numCols = buf.getShort();
            for (int i = 0; i < numCols; i++) {
                short columnId = buf.getShort();
                int typeTag = buf.get();
                int dataOffset = buf.getInt();
                int dataLength = buf.getInt();
                dir.put(columnId, new int[]{typeTag, dataOffset, dataLength});
            }
        }

        void ensureStringIndex(short columnId) {
            if (strStarts.containsKey(columnId)) {
                return;
            }
            int[] meta = dir.get(columnId);
            int pos = meta[1];
            int end = meta[1] + meta[2];
            List<Integer> starts = new ArrayList<>();
            List<Integer> lens = new ArrayList<>();
            while (pos < end) {
                int len = buf.getInt(pos);
                pos += 4;
                starts.add(pos);
                lens.add(len);
                pos += len;
            }
            int[] s = new int[starts.size()];
            int[] l = new int[lens.size()];
            for (int i = 0; i < s.length; i++) {
                s[i] = starts.get(i);
                l[i] = lens.get(i);
            }
            strStarts.put(columnId, s);
            strLens.put(columnId, l);
        }
    }

    /** Random-access view of one row; reused across rows for cheap pushdown. */
    private final class RowView implements RowAccessor {
        final Decoded d;
        int row;

        RowView(Decoded d) {
            this.d = d;
        }

        @Override
        public Object get(short columnId) {
            ColumnType t = ColumnType.fromTag((byte) d.dir.get(columnId)[0]);
            switch (t) {
                case INT:
                    return getInt(columnId);
                case LONG:
                    return getLong(columnId);
                case STRING:
                    return getString(columnId);
                default:
                    throw new IllegalStateException();
            }
        }

        @Override
        public int getInt(short columnId) {
            int off = d.dir.get(columnId)[1] + row * 4;
            return d.buf.getInt(off);
        }

        @Override
        public long getLong(short columnId) {
            int off = d.dir.get(columnId)[1] + row * 8;
            return d.buf.getLong(off);
        }

        @Override
        public String getString(short columnId) {
            d.ensureStringIndex(columnId);
            int start = d.strStarts.get(columnId)[row];
            int len = d.strLens.get(columnId)[row];
            byte[] b = new byte[len];
            for (int i = 0; i < len; i++) {
                b[i] = d.buf.get(start + i);
            }
            return new String(b, StandardCharsets.UTF_8);
        }
    }

    // ------------------------------------------------------------------ ranges

    @Override
    public List<ColumnRange> computeRanges(List<Record> rows, List<Short> indexedColumns) {
        List<ColumnRange> out = new ArrayList<>();
        for (short columnId : indexedColumns) {
            ColumnType type = typeOf(columnId);
            Object min = null;
            Object max = null;
            for (Record r : rows) {
                Object v = valueOf(r, columnId);
                if (min == null || cmp(v, min, type) < 0) {
                    min = v;
                }
                if (max == null || cmp(v, max, type) > 0) {
                    max = v;
                }
            }
            out.add(new ColumnRange(columnId,
                    type.encodeOrderPreserving(min),
                    type.encodeOrderPreserving(max)));
        }
        return out;
    }

    @Override
    public long[] eventTimeMinMax(List<Record> rows) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (Record r : rows) {
            min = Math.min(min, r.eventTime);
            max = Math.max(max, r.eventTime);
        }
        return new long[]{min, max};
    }

    @SuppressWarnings("unchecked")
    private static int cmp(Object a, Object b, ColumnType t) {
        switch (t) {
            case INT:
                return Integer.compare(((Number) a).intValue(), ((Number) b).intValue());
            case LONG:
                return Long.compare(((Number) a).longValue(), ((Number) b).longValue());
            case STRING:
                return ((String) a).compareTo((String) b);
            default:
                throw new IllegalStateException();
        }
    }

    // ------------------------------------------------------------------ helpers

    private static void writeInt(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void writeLong(ByteArrayOutputStream out, long v) {
        for (int i = 7; i >= 0; i--) {
            out.write((int) ((v >>> (i * 8)) & 0xFF));
        }
    }
}
