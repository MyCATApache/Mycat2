package io.mycat.ext;

import io.mycat.mysqlapi.callback.MySQLAPISessionCallback;
import io.mycat.mysqlapi.callback.MySQLJobCallback;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import java.util.Collections;

public class MySQLAPIRuntimeImpl implements io.mycat.mysqlapi.MySQLAPIRuntime {

  @Override
  public void create(String dataSourceName, MySQLAPISessionCallback callback) {
    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
    ProxyRuntime runtime = thread.getRuntime();
    thread.getMySQLSessionManager().getIdleSessionsOfIds(
        runtime.getDataSourceByDataSourceName(dataSourceName),
        Collections.emptyList(), new SessionCallBack<MySQLClientSession>() {
          @Override
          public void onSession(MySQLClientSession session, Object sender, Object attr) {
            MySQLAPIImpl mySQLAPI = new MySQLAPIImpl(session);
            callback.onSession(mySQLAPI);
          }

          @Override
          public void onException(Exception exception, Object sender, Object attr) {
            callback.onException(exception);
          }
        });
  }

  @Override
  public void addPengdingJob(MySQLJobCallback callback) {
    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
    thread.addNIOJob(new NIOJob() {
      @Override
      public void run(ReactorEnvThread reactor) throws Exception {
        callback.run();
      }

      @Override
      public void stop(ReactorEnvThread reactor, Exception reason) {
        callback.stop(reason);
      }

      @Override
      public String message() {
        return callback.message();
      }
    });
  }
}