package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.schema.BaseColumnDefinition;

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