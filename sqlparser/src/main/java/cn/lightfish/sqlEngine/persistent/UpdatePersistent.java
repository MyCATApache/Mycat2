package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.schema.MycatTable;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

import java.util.Iterator;

public class UpdatePersistent implements QueryPersistent {
    private final MycatTable table;
    private final Iterator<Object[]> rows;


    public UpdatePersistent(MycatTable table, Iterator<Object[]> rows) {
        this.table = table;
        this.rows = rows;
    }

    @Override
    public TableColumnDefinition[] columnDefList() {
        return table.getColumnDefinitions();
    }

    @Override
    public boolean hasNext() {
        return rows.hasNext();
    }

    @Override
    public MycatTable getTable() {
        return table;
    }

    @Override
    public Object[] next() {
        return rows.next();
    }

    public void delete() {
        rows.remove();
    }
}