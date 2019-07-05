package io.mycat.mysqlapi;

import io.mycat.mysqlapi.callback.MySQLAPISessionCallback;
import io.mycat.mysqlapi.callback.MySQLJobCallback;

public interface MySQLAPIRuntime {

  void create(String dataSourceName, MySQLAPISessionCallback callback);

  void addPengdingJob(MySQLJobCallback callback);
}