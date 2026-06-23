package io.streamlake;

/** A single schema field with a stable columnId (IDs never change for a topic). */
public final class Field {
    public final short columnId;
    public final String name;
    public final ColumnType type;

    public Field(short columnId, String name, ColumnType type) {
        this.columnId = columnId;
        this.name = name;
        this.type = type;
    }

    @Override
    public String toString() {
        return name + "(" + columnId + ":" + type + ")";
    }
}
