package io.streamlake;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Topic schema. Required for Streaming-Lake (columnar) topics so the broker can
 * transpose rows into columns and encode them. Each field has a stable columnId.
 */
public final class SchemaDef {
    private final List<Field> fields = new ArrayList<>();
    private final Map<String, Field> byName = new LinkedHashMap<>();
    private final Map<Short, Field> byId = new LinkedHashMap<>();

    public SchemaDef field(short columnId, String name, ColumnType type) {
        Field f = new Field(columnId, name, type);
        fields.add(f);
        byName.put(name, f);
        byId.put(columnId, f);
        return this;
    }

    public List<Field> fields() {
        return fields;
    }

    public Field byName(String name) {
        Field f = byName.get(name);
        if (f == null) {
            throw new IllegalArgumentException("Unknown field: " + name);
        }
        return f;
    }

    public Field byId(short columnId) {
        Field f = byId.get(columnId);
        if (f == null) {
            throw new IllegalArgumentException("Unknown columnId: " + columnId);
        }
        return f;
    }

    public int size() {
        return fields.size();
    }
}
