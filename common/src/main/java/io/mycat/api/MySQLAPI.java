package io.mycat.api;

import io.mycat.api.callback.MySQLAPIExceptionCallback;
import io.mycat.api.collector.ResultSetCollector;

public interface MySQLAPI {

  void query(String sql, ResultSetCollector resultSetCollector,
      MySQLAPIExceptionCallback exceptionCollector);


  void close();
}