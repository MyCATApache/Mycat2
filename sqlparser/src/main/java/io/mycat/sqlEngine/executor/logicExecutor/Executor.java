package io.mycat.sqlEngine.executor.logicExecutor;

import io.mycat.sqlEngine.schema.BaseColumnDefinition;
import io.mycat.sqlEngine.schema.DbTable;
import io.mycat.sqlEngine.schema.TableColumnDefinition;

import java.util.Iterator;

public interface Executor extends Iterator<Object[]> {
    default void apply() {

    }

    default BaseColumnDefinition[] columnDefList() {
        return new TableColumnDefinition[]{};
    }

    boolean hasNext();

    Object[] next();

    default void delete() {
    }

    public default DbTable getTable() {
        return null;
    }
}