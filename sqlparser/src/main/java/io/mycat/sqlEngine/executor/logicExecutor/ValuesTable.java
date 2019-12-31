package io.mycat.sqlEngine.executor.logicExecutor;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;

public class ValuesTable implements Executor {
    private final String tableName;
    private final BaseColumnDefinition[] columnDefinitions;
    private final ValueExpr[][] rows;

    public ValuesTable(String tableName, BaseColumnDefinition[] columnDefinitions, ValueExpr[][] rows) {
        this.tableName = tableName;
        this.columnDefinitions = columnDefinitions;
        this.rows = rows;
    }

    @Override
    public BaseColumnDefinition[] columnDefList() {
        return columnDefinitions;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Object[] next() {
        return new Object[0];
    }
}