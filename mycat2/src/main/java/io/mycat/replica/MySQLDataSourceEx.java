package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.task.client.MySQLTaskUtil;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221
 * @date 2019-05-14 19:36
 **/
public class MySQLDataSourceEx extends MySQLDatasource {

  final AtomicInteger heartBeatFail = new AtomicInteger(0);

  public MySQLDataSourceEx(int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    super(index, datasourceConfig, replica);
  }

  public void heartBeat() {
    MySQLTaskUtil.getMySQLSessionForHeartbeatFromUserThread(this,
        (session, sender, success, result, attr) -> {
          if (success) {
            session.ping((session1, sender1, success1, result1, attr1) -> {
              if (success1) {
                MySQLSessionManager sessionManager = session1.getSessionManager();
                System.out.println("heartbeat :" + sessionManager.currentSessionCount());
                if (!this.isAlive()) {
                  this.heartBeatFail.set(0);
                }
                sessionManager.addIdleSession(session1);
              } else {
                heartBeatFail.incrementAndGet();
                System.out.println("heartbeat fail");
              }
            });
          } else {
            heartBeatFail.incrementAndGet();
            System.out.println("heartbeat fail");
          }
        });
  }

  @Override
  public boolean isAlive() {
    return heartBeatFail.get() < 3;
  }
}
