package io.mycat.proxy.handler.backend;

import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.session.MySQLClientSession;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-22 11:13
 **/
public enum RequestTask implements NIOHandler<MySQLClientSession> {
  INSTANCE;

  public void request(MySQLClientSession session, byte[] packet, RequestCallback callback) {
    session.setCallBack(callback);
    try {
      session.writeProxyBufferToChannel(packet);
    } catch (Exception e) {
      onClear(session);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onSocketRead(MySQLClientSession session) {
    Exception e = null;
    try {
      session.readFromChannel();
    } catch (Exception e1) {
      e = e1;
    }
    RequestCallback callback = session.getCallBack();
    session.setCallBack(null);
    onClear(session);
    callback.onFinishedSendException(e, this, null);
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {
    try {
      session.writeToChannel();
    } catch (IOException e) {
      RequestCallback callback = session.getCallBack();
      session.setCallBack(null);
      onClear(session);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onWriteFinished(MySQLClientSession session) {
    RequestCallback callback = session.getCallBack();
    session.setCallBack(null);
    onClear(session);
    callback.onFinishedSend(session, this, null);
  }

  public void onClear(MySQLClientSession session) {
    session.resetPacket();
    session.setCallBack(null);
    session.switchNioHandler(null);
  }
}
