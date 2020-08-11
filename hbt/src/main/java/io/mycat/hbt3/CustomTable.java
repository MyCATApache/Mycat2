package io.mycat.hbt3;

import io.mycat.hbt4.Executor;
import org.apache.calcite.rex.RexNode;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public abstract class CustomTable {
    final String schemaName;
    final String tableName;
    final String createTableSQL;
    final Map<String, String> kvOptions;
    final List<String> listOptions;

    public CustomTable(String schemaName,
                       String tableName,
                       String createTableSQL,
                       Map<String, String> kvOptions,
                       List<String> listOptions) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.createTableSQL = createTableSQL;
        this.kvOptions = kvOptions;
        this.listOptions = listOptions;
    }

    abstract Executor scan(int[] project, List<RexNode> conditions, Limit limit);

    public static class Limit {
        Comparator comparator;
        Long offset;
        Long limit;
    }
}