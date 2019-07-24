package io.mycat.proxy.handler.backend;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MySQLClientSession;

public class MySQLSessionSyncUtil {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(MySQLSessionSyncUtil.class);

  public static void sync(MySQLSynContext mycatContext, MySQLClientSession mysql, Object sender,
      SessionSyncCallback callBack) {
    MySQLSynContext mySQLSynContext = mysql.getRuntime().getProviders()
        .createMySQLSynContext(mysql);
    if (mySQLSynContext.equals(mycatContext)) {
      callBack.onSession(mysql, sender, null);
      return;
    } else {
      String syncSQL = mycatContext.getSyncSQL();
      MycatMonitor.onSyncSQL(mycatContext, syncSQL, mysql);
      ResultSetHandler.DEFAULT.request(mysql, MySQLCommandType.COM_QUERY, syncSQL,
          new ResultSetCallBack<MySQLClientSession>() {

            @Override
            public void onFinished(boolean monopolize, MySQLClientSession mysql,
                Object sender, Object attr) {
              mycatContext.successSyncMySQLClientSession(mysql);
              mycatContext.onSynchronizationStateLog(mysql);
              callBack.onSession(mysql, sender, attr);
            }

            @Override
            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                MySQLClientSession mysql, Object sender, Object attr) {
              String messageString = errorPacket.getErrorMessageString();
              LOGGER.error("sync session fail:{}",messageString);
              mysql.close(false, messageString);
              if (monopolize) {
                callBack.onException(new MycatException(messageString), this, null);
                return;
              } else {
                callBack.onErrorPacket(errorPacket, false, mysql, this, null);
                return;
              }
            }

            @Override
            public void onFinishedSendException(Exception exception, Object sender,
                Object attr) {
              LOGGER.error("sync session fail:",exception);
              callBack.onException(exception, sender, attr);
            }

            @Override
            public void onFinishedException(Exception exception, Object sender, Object attr) {
              LOGGER.error("sync session fail:",exception);
              callBack.onException(exception, sender, attr);
            }
          });
    }
  }
}