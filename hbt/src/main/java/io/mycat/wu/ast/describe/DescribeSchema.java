package io.mycat.wu.ast.describe;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DescribeSchema {
    String databaseName;
    String catalogName;
    String schemaName;
    String tableName;
    String columnName;
}