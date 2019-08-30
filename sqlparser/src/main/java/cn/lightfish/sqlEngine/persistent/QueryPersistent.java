package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.executor.logicExecutor.Executor;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

public interface QueryPersistent extends Executor {



  public default TableColumnDefinition[] columnDefList(){
    return new TableColumnDefinition[0];
  }


  public boolean hasNext();

  public Object[] next();
}