package io.mycat.api;

import io.mycat.api.callback.MySQLAPISessionCallback;
import io.mycat.api.callback.MySQLJobCallback;

public interface MySQLAPIRuntime {

  void create(String dataSourceName, MySQLAPISessionCallback callback);

  void addPengdingJob(MySQLJobCallback callback);
}