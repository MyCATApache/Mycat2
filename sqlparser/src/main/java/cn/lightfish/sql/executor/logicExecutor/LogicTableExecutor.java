package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.SimpleColumnDefinition;

public class LogicTableExecutor extends AbsractExecutor {

  public Executor physicsExecutor;
  public MycatTable table;

  public LogicTableExecutor(SimpleColumnDefinition[] columnList,
      MycatTable table) {
    super(columnList);
    this.table = table;
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
}