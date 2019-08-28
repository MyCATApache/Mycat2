package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.schema.BaseColumnDefinition;

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