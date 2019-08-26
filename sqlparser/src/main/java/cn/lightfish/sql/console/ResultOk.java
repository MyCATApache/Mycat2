package cn.lightfish.sql.console;

import java.util.Iterator;
import java.util.List;

public class ResultOk implements MycatConsoleResult {
  String sql;

  public ResultOk() { ;
  }


  public String sql() {
    return sql;
  }

  @Override
  public List<String> columnDefList() {
    return null;
  }

  @Override
  public Iterator<Object[]> rowIterator() {
    return null;
  }
}