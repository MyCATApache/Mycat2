package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.executor.DefExecutor;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;

import java.util.ArrayList;

public class ExecutorTable {
    String tableName;
    private final ArrayList<Object[]> rows = new ArrayList<>();
    private final Executor executor;

    public ExecutorTable(String tableName, Executor executor) {
        this.tableName = tableName;
        this.executor = executor;
    }

    public void apply() {
        while (executor.hasNext()) {
            rows.add(executor.next());
        }
    }

    public BaseColumnDefinition[] columnDefList() {
        return executor.columnDefList();
    }

    public Executor iterator() {
        return new DefExecutor(executor.columnDefList(), rows.listIterator());
    }

    public Object getScalarValue() {
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new UnsupportedOperationException();
    }

    public boolean exists() {
        return !rows.isEmpty();
    }

    public Object[] getRowValues() {
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new UnsupportedOperationException();
    }

    public Object[] getColumnValues() {
        Object[] res = new Object[rows.size()];
        for (int i = 0; i < res.length; i++) {
            Object[] row = rows.get(i);
            if (row.length == 1) {
                res[i] = row[0];
            } else {
                throw new UnsupportedOperationException();
            }
        }
        return res;
    }
}