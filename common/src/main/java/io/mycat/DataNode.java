package io.mycat;

public interface DataNode {
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

    default  public String getUniqueName() {
        return getTargetName() + "." + getTargetSchemaTable();
    }
}