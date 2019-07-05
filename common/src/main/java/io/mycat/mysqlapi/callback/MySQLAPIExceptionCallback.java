package io.mycat.mysqlapi.callback;

import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.mysqlapi.MySQLAPI;

public interface MySQLAPIExceptionCallback {

  void onException(Exception exception, MySQLAPI mySQLAPI);

  void onFinished(boolean monopolize, MySQLAPI mySQLAPI);

  void onErrorPacket(ErrorPacket errorPacket, boolean monopolize,
      MySQLAPI mySQLAPI);
}