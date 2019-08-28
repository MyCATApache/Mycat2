package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.BaseColumnDefinition;

import java.util.Iterator;

public interface Executor extends Iterator<Object[]> {
    default public void apply() {

    }

    BaseColumnDefinition[] columnDefList();

    boolean hasNext();

    Object[] next();
}