package io.streamlake;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single application row (one Pulsar message before columnar batching).
 * Carries an eventTime (used for date partitioning) and one value per schema field.
 */
public final class Record implements RowAccessor {
    public final long eventTime;
    private final Map<Short, Object> values = new LinkedHashMap<>();

    public Record(long eventTime) {
        this.eventTime = eventTime;
    }

    public Record put(short columnId, Object value) {
        values.put(columnId, value);
        return this;
    }

    public Object get(short columnId) {
        return values.get(columnId);
    }

    public int getInt(short columnId) {
        return ((Number) values.get(columnId)).intValue();
    }

    public long getLong(short columnId) {
        return ((Number) values.get(columnId)).longValue();
    }

    public String getString(short columnId) {
        return (String) values.get(columnId);
    }

    @Override
    public String toString() {
        return "Record" + values + "@" + eventTime;
    }
}
