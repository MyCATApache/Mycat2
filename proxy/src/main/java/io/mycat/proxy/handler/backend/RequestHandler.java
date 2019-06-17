package io.mycat.proxy.handler.backend;

import io.mycat.MycatExpection;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 *  date 2019-05-22 11:13
 **/
public enum RequestHandler implements NIOHandler<MySQLClientSession> {
  INSTANCE;
  protected final static Logger logger = LoggerFactory.getLogger(BackendConCreateHandler.class);
  public void request(MySQLClientSession session, byte[] packet, RequestCallback callback) {
    session.setCallBack(callback);
    try {
      session.setCallBack(callback);
      session.switchNioHandler(this);
      MycatReactorThread thread = (MycatReactorThread)Thread.currentThread();
      session.setCurrentProxyBuffer(new ProxyBufferImpl(thread.getBufPool()));
      session.writeProxyBufferToChannel(packet);
    } catch (Exception e) {
      MycatMonitor.onRequestException(session,e);
      onException(session, e);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onSocketRead(MySQLClientSession session) {
    Exception e = new MycatExpection("unknown read data");
    try {
      session.readFromChannel();
    } catch (Exception e1) {
      e = e1;
    }
    MycatMonitor.onRequestException(session,e);
    RequestCallback callback = session.getCallBack();
    onException(session, e);
    callback.onFinishedSendException(e, this, null);
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {
    try {
      session.writeToChannel();
    } catch (Exception e) {
      MycatMonitor.onRequestException(session,e);
      RequestCallback callback = session.getCallBack();
      onException(session, e);
      callback.onFinishedSendException(e, this, null);
    }
  }

  @Override
  public void onWriteFinished(MySQLClientSession session) {
    RequestCallback callback = session.getCallBack();
    onClear(session);
    callback.onFinishedSend(session, this, null);
  }

  @Override
  public void onException(MySQLClientSession session, Exception e) {
    MycatMonitor.onRequestException(session,e);
    logger.error("{}", e);
    onClear(session);
    session.setCallBack(null);
    session.close(false, e);
  }

  public void onClear(MySQLClientSession session) {
    session.resetPacket();
    session.setCallBack(null);
    session.switchNioHandler(null);
    MycatMonitor.onRequestClear(session);
  }
}
