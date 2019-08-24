package io.mycat.sqlparser;

import java.util.Iterator;
import java.util.List;

public interface MycatConsoleResult {
  List<String> columnDefList();

  Iterator<Object[]> rowIterator();
}