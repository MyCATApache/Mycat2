package io.mycat.command.prepareStatement;

import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;

/**
 * @author jamie12221
 *  date 2019-04-30 16:24
 *  后端mysql的预处理响应回调
 **/
public interface PrepareSessionCallback {
  void onPrepare(long actualStatementId, MySQLClientSession session);

  void onException(Exception exception, Object sender, Object attr);
  void onErrorPacket(ErrorPacketImpl errorPacket,boolean monopolize, MySQLClientSession mysql, Object sender, Object attr);
}