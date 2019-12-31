package io.mycat.sqlEngine.executor.logicExecutor;

import io.mycat.sqlEngine.schema.DbTable;
import io.mycat.sqlEngine.schema.TableColumnDefinition;

import java.util.Map;

public class LogicLeafTableExecutor implements Executor {

    public Executor physicsExecutor;
    public DbTable table;
    public Map<String, Object> persistentAttribute;
    protected final TableColumnDefinition[] columnList;
    private final ExecutorType type;

    public LogicLeafTableExecutor(TableColumnDefinition[] columnList,
                                  DbTable table, Map<String, Object> persistentAttribute, ExecutorType type) {
        this.columnList = columnList;
        this.table = table;
        this.persistentAttribute = persistentAttribute;
        this.type = type;
    }

    @Override
    public TableColumnDefinition[] columnDefList() {
        return columnList;
    }

    @Override
    public boolean hasNext() {
        return physicsExecutor.hasNext();
    }

    @Override
    public Object[] next() {
        return physicsExecutor.next();
    }

    public void delete() {
        physicsExecutor.delete();
    }

    public void setPhysicsExecutor(Executor physicsExecutor) {
        this.physicsExecutor = physicsExecutor;
    }

    @Override
    public DbTable getTable() {
        return table;
    }

    public void putAttribute(String name, Object attribute) {
        persistentAttribute.put(name, attribute);
    }

    public Map<String, Object> getPersistentAttribute() {
        return persistentAttribute;
    }

    public ExecutorType getType() {
        return type;
    }
}