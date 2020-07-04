package io.mycat;

public interface DataNode extends java.lang.Comparable<DataNode> {
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
        String targetName = getTargetName();
        if (targetName == null) {
            return getTargetSchemaTable();
        }
        return targetName + "." + getTargetSchemaTable();
    }

    default public int compareTo(DataNode o) {
        return (this.getTargetName().compareTo(o.getTargetName()));
    }
}