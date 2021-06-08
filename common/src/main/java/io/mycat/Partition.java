package io.mycat;

public interface Partition extends java.lang.Comparable<Partition> {
    String getTargetName();

    String getSchema();

    String getTable();

    default String getTargetSchemaTable() {
        String schema = getSchema();
        if (schema == null) {
            return getTable();
        }
        return schema + "." + getTable();
    }

    default public String getUniqueName() {
        return getTargetName() + "." + getTargetSchemaTable();
    }

    default public int compareTo(Partition o) {
        return (this.getTargetName().compareTo(o.getTargetName()));
    }
}