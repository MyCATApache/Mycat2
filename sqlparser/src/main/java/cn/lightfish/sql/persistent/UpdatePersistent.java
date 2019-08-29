package cn.lightfish.sql.persistent;

import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.TableColumnDefinition;

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