package cn.lightfish.sql.persistent;

import cn.lightfish.sql.context.Navigation;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.TableColumnDefinition;
import java.util.Iterator;

public interface QueryPersistent extends Executor {



  public default TableColumnDefinition[] columnDefList(){
    return new TableColumnDefinition[0];
  }


  public boolean hasNext();

  public Object[] next();
}