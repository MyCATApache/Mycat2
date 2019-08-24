package cn.lightfish.sql.ast;

import io.mycat.schema.MycatColumnDefinition;
import java.util.List;

public interface Executor {

  MycatColumnDefinition[] columnDefList();

  boolean hasNext();

  Object[] next();
}