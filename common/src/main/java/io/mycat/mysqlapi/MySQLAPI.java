package io.mycat.mysqlapi;

import io.mycat.mysqlapi.callback.MySQLAPIExceptionCallback;
import io.mycat.mysqlapi.collector.ResultSetCollector;

public interface MySQLAPI {

  void query(String sql, ResultSetCollector resultSetCollector,
      MySQLAPIExceptionCallback exceptionCollector);

  void close();
}