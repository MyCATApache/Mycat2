package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.schema.MycatTable;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

import java.util.Map;

public class LogicLeafTableExecutor implements Executor {

    public Executor physicsExecutor;
    public MycatTable table;
    public Map<String, Object> persistentAttribute;
    protected final TableColumnDefinition[] columnList;
    private final ExecutorType type;

    public LogicLeafTableExecutor(TableColumnDefinition[] columnList,
                                  MycatTable table, Map<String, Object> persistentAttribute, ExecutorType type) {
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
    public MycatTable getTable() {
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