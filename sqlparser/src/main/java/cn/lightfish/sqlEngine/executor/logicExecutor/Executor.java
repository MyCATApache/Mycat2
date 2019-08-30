package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;
import cn.lightfish.sqlEngine.schema.MycatTable;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

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

    public default MycatTable getTable() {
        return null;
    }
}