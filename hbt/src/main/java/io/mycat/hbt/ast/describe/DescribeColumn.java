package io.mycat.hbt.ast.describe;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DescribeColumn {
    String databaseName;
    String catalogName;
    String schemaName;
    String tableName;
    String columnName;
}