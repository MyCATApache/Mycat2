package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;

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