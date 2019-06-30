package io.mycat.proxy.callback;

import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;

public interface CommandCallBack extends TaskCallBack<CommandCallBack> {

  void onFinishedOk(int serverStatus, MySQLClientSession session,
      Object sender,
      Object attr);

  void onFinishedException(Exception exception, Object sender, Object attr);

  void onFinishedErrorPacket(ErrorPacketImpl errorPacket, int lastServerStatus,
      MySQLClientSession session,
      Object sender,
      Object attr);
}
