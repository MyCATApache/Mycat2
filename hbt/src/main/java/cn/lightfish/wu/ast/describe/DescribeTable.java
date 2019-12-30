package cn.lightfish.wu.ast.describe;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DescribeTable {
    String databaseName;
    String catalogName;
    String schemaName;
    String tableName;
}