package io.mycat.rsqlBuilder.schema;

public interface DataSource {
    ColumnObject getColumn(String columnName);
}