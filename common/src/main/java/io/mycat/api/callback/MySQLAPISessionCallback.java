package io.mycat.api.callback;

import io.mycat.api.MySQLAPI;

public interface MySQLAPISessionCallback {

  void onSession(MySQLAPI mySQLAPI);

  void onException(Exception exception);
}