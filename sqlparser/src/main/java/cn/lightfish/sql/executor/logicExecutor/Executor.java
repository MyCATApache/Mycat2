package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.SimpleColumnDefinition;
import java.util.Iterator;

public interface Executor extends Iterator<Object[]> {

  SimpleColumnDefinition[] columnDefList();

  boolean hasNext();

  Object[] next();
}