package io.mycat.proxy.handler.backend;

import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;

public interface SessionSyncCallback {
  void onSession(MySQLClientSession session, Object sender, Object attr);
  void onException(Exception exception, Object sender, Object attr);
  void onErrorPacket(ErrorPacketImpl errorPacket,boolean monopolize, MySQLClientSession mysql, Object sender, Object attr);
}