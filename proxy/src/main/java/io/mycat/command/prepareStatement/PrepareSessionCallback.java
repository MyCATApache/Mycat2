package io.mycat.command.prepareStatement;

import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;

public interface PrepareSessionCallback {
  void onPrepare(long actualStatementId, MySQLClientSession session);

  void onException(Exception exception, Object sender, Object attr);
  void onErrorPacket(ErrorPacketImpl errorPacket,boolean monopolize, MySQLClientSession mysql, Object sender, Object attr);
}