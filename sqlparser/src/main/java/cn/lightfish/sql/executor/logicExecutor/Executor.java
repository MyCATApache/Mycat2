package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.BaseColumnDefinition;
import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.TableColumnDefinition;

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