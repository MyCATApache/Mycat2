package cn.lightfish.rsqlBuilder.schema;

public interface DataSource {
    ColumnObject getColumn(String columnName);
}