package io.mycat;

public interface DataNode extends java.lang.Comparable<DataNode> {
    String getTargetName();

    String getSchema();

    String geTable();

    default String getTargetSchemaTable() {
        String schema = getSchema();
        if (schema == null) {
            return geTable();
        }
        return schema + "." + geTable();
    }

    default public String getUniqueName() {
        return getTargetName() + "." + getTargetSchemaTable();
    }

    default public int compareTo(DataNode o) {
        return (this.getTargetName().compareTo(o.getTargetName()));
    }
}