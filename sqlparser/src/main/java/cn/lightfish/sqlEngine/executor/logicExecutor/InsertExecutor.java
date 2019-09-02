package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.persistent.InsertPersistent;
import cn.lightfish.sqlEngine.schema.DbTable;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

import java.util.Iterator;
import java.util.Map;

public class InsertExecutor implements Executor {
    private final DbTable table;
    private final TableColumnDefinition[] columnNameList;
    private final Map<String, Object> persistentAttribute;
    InsertPersistent insertPersistent;
   final Iterator<Object[]> row;


    public InsertExecutor(DbTable table, TableColumnDefinition[] columnNameList, Map<String, Object> persistentAttribute, Iterator<Object[]> iterator) {
        this.table = table;
        this.columnNameList = columnNameList;
        this.persistentAttribute = persistentAttribute;
        this.row = iterator;
    }

    @Override
    public boolean hasNext() {
        return row.hasNext();
    }

    @Override
    public Object[] next() {
        while (row.hasNext()){
            insertPersistent.insert(row.next());
        }
        return new Object[0];
    }

    public void setInsertPersistent(InsertPersistent insertPersistent) {
        this.insertPersistent = insertPersistent;
    }

    @Override
    public DbTable getTable() {
        return table;
    }

    public TableColumnDefinition[] getColumnNameList() {
        return columnNameList;
    }

    public Map<String, Object> getPersistentAttribute() {
        return persistentAttribute;
    }

    public InsertPersistent getInsertPersistent() {
        return insertPersistent;
    }

    public Iterator<Object[]> getRow() {
        return row;
    }
}