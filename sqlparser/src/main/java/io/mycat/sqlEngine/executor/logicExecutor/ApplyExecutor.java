package io.mycat.sqlEngine.executor.logicExecutor;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;

public class ApplyExecutor implements Executor {
    final ExecutorTable executor;
    final ValueExpr[] outColumnExpr;
    final ValueExpr whereExpr;
    Executor iterator;

    public void apply() {
        iterator = executor.iterator();
    }

    public ApplyExecutor(ExecutorTable executor, ValueExpr[] outColumnExpr, ValueExpr where) {
        this.executor = executor;
        this.outColumnExpr = outColumnExpr;
        this.whereExpr = where;
    }

    @Override
    public BaseColumnDefinition[] columnDefList() {
        return executor.columnDefList();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Object[] next() {
        return iterator.next();
    }
}