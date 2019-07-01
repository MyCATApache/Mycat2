package io.mycat.proxy.handler.backend;


import io.mycat.MycatException;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MySQLClientSession;

public enum IdleHandler implements NIOHandler<MySQLClientSession> {
  INSTANCE;
  protected final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(
      IdleHandler.class);

  @Override
  public void onSocketRead(MySQLClientSession session) {
    MycatMonitor.onIdleReadException(session,
        new MycatException("mysql session:{} is idle but it receive response", session));
    session.close(false, "mysql session  is idle but it receive response");
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {

  }

  @Override
  public void onWriteFinished(MySQLClientSession session) {
    assert false;
  }

  @Override
  public void onException(MySQLClientSession session, Exception e) {
    LOGGER.error("{}", e);
    session.close(false, e);
  }

}