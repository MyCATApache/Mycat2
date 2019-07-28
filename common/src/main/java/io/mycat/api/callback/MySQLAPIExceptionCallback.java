package io.mycat.api.callback;

import io.mycat.api.MySQLAPI;
import io.mycat.beans.mysql.packet.ErrorPacket;

public interface MySQLAPIExceptionCallback {

  void onException(Exception exception, MySQLAPI mySQLAPI);

  void onFinished(boolean monopolize, MySQLAPI mySQLAPI);

  void onErrorPacket(ErrorPacket errorPacket, boolean monopolize,
      MySQLAPI mySQLAPI);
}