package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.TableColumnDefinition;
import java.util.Map;

public class LogicLeafTableExecutor implements Executor {

  public Executor physicsExecutor;
  public MycatTable table;
  public Map<String, Object> persistentAttribute;
  protected final TableColumnDefinition[] columnList;

  public LogicLeafTableExecutor(TableColumnDefinition[] columnList,
      MycatTable table, Map<String, Object> persistentAttribute) {
    this.columnList = columnList;
    this.table = table;
    this.persistentAttribute = persistentAttribute;
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

  public void setPhysicsExecutor(Executor physicsExecutor) {
    this.physicsExecutor = physicsExecutor;
  }

  public MycatTable getTable() {
    return table;
  }

  public void putAttribute(String name, Object attribute) {
    persistentAttribute.put(name, attribute);
  }

  public Map<String, Object> getPersistentAttribute() {
    return persistentAttribute;
  }
}