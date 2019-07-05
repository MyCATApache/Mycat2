package io.mycat.mysqlapi.callback;

import io.mycat.mysqlapi.MySQLAPI;

public interface MySQLAPISessionCallback {

  void onSession(MySQLAPI mySQLAPI);

  void onException(Exception exception);
}