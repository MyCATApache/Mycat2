package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.persistent.UpdatePersistent;
import cn.lightfish.sqlEngine.schema.DbTable;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

import java.util.Map;

public class DeleteExecutor implements Executor {
    private TableColumnDefinition[] tableColumnDefinitions;
    final FilterExecutor filter;
    private Map<String, Object> map;
    UpdatePersistent updatePersistent;
    public DbTable table;
    public DeleteExecutor(TableColumnDefinition[] tableColumnDefinitions, DbTable table, FilterExecutor filter, Map<String,Object> map) {
        this.tableColumnDefinitions = tableColumnDefinitions;
        this.table = table;
        this.filter = filter;
        this.map = map;
    }

    @Override
    public TableColumnDefinition[] columnDefList() {
        return tableColumnDefinitions;
    }

    @Override
    public boolean hasNext() {
        return filter.hasNext();
    }

    @Override
    public Object[] next() {
           updatePersistent.delete();
        return new Object[0];
    }

    @Override
    public DbTable getTable() {
        return table;
    }

    public void setUpdatePersistent(UpdatePersistent updatePersistent) {
        this.updatePersistent = updatePersistent;
    }

    public Map<String,Object> getPersistentAttribute() {
        return map;
    }
}