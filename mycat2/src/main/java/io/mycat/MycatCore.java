/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.GlobalConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.logTip.ReplicaTip;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;
import io.mycat.proxy.monitor.ProxyDashboard;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.MySQLDataSourceEx;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author cjw
 **/
public class MycatCore {

  private static final Logger LOGGER = LoggerFactory.getLogger(MycatCore.class);

  private static ProxyRuntime runtime;

  public static void main(String[] args) throws Exception {
    String resourcesPath = System.getProperty("MYCAT_HOME");
    LOGGER.info("MYCAT_HOME:{}", resourcesPath);
    if (resourcesPath == null) {
      resourcesPath = ProxyRuntime.getResourcesPath(MycatCore.class);
    }
    MycatProxyBeanProviders proxyBeanProviders = new MycatProxyBeanProviders();
    runtime = new ProxyRuntime(
        ConfigLoader.load(resourcesPath, GlobalConfig.genVersion()), proxyBeanProviders);
    startup(resourcesPath, runtime, new MycatMonitorLogCallback(),
        new AsyncTaskCallBack() {
          @Override
          public void onFinished(Object sender, Object result, Object attr) {

          }

          @Override
          public void onException(Exception e, Object sender, Object attr) {
            e.printStackTrace();
          }

        });
    return;

  }

  public static void startup(String resourcesPath, ProxyRuntime runtime,
      MycatMonitorCallback callback,
      AsyncTaskCallBack startFinished)
      throws IOException {
    try {
      MycatMonitor.setCallback(callback);
      runtime.startReactor();
      ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
      HeartbeatRootConfig heartbeatRootConfig = runtime
          .getConfig(ConfigEnum.HEARTBEAT);
      long idleTimeout = heartbeatRootConfig.getHeartbeat().getIdleTimeout();
      long replicaIdleCheckPeriod = idleTimeout / 2;
      service.scheduleAtFixedRate(idleConnectCheck(runtime), 0, replicaIdleCheckPeriod,
          TimeUnit.SECONDS);
      long period = heartbeatRootConfig.getHeartbeat().getReplicaHeartbeatPeriod();
      service.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          Collection<MySQLDataSourceEx> datasourceList = runtime.getMySQLDatasourceList();
          for (MySQLDataSourceEx datasource : datasourceList) {
            datasource.heartBeat();
          }
        }
      }, 0, period, TimeUnit.SECONDS);
      service.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {

          try {
            ProxyDashboard.INSTANCE.collectInfo(runtime);
          } catch (Exception e) {
            e.printStackTrace();
          }

        }
      }, 0, 5, TimeUnit.SECONDS);

      runtime.startAcceptor();
      startFinished.onFinished(null, null, null);
    } catch (Exception e) {
      startFinished.onException(e, null, null);
    }

//
//  public static void getReplicaMetaData(ProxyRuntime runtime, AsyncTaskCallBack asyncTaskCallBack) {
//    Collection<MySQLReplica> mySQLReplicaList = runtime.getMySQLReplicaList();
//    AsyncTaskCallBackCounter counter = new AsyncTaskCallBackCounter(mySQLReplicaList.size(),
//        asyncTaskCallBack);
//    for (MySQLReplica mySQLReplica : mySQLReplicaList) {
//      MySQLDatasource master = mySQLReplica.getMaster();
//      MySQLTaskUtil.getMySQLSessionForTryConnect(master,
//          new AsyncTaskCallBack<MySQLClientSession>() {
//            @Override
//            public void finished(MySQLClientSession session, Object sender,
//                boolean success,
//                Object result, Object attr) {
//              QueryUtil.showInformationSchemaColumns(session,
//                  new AsyncTaskCallBack<MySQLClientSession>() {
//                    @Override
//                    public void finished(MySQLClientSession session, Object sender,
//                        boolean success, Object result, Object attr) {
//                      if (success) {
//                        session.getSessionManager().addIdleSession(session);
//                        ResultSetCollector collector = (ResultSetCollector) result;
//                        for (Object[] objects : collector) {
//                          String TABLE_SCHEMA = (String) objects[1];
//                          String TABLE_NAME = (String) objects[2];
//                          String COLUMN_NAME = (String) objects[3];
//                          Object CHARACTER_OCTET_LENGTH = objects[9];
//                          Object COLUMN_TYPE = objects[15];
//                          mySQLReplica
//                              .addMetaData(TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME);
//                        }
//                        counter.finished(session, sender, success, result, attr);
//                      } else {
//                        counter.finished(session, sender, success, result, attr);
//                      }
//                    }
//                  });
//            }
//          });
//    }
//  }
//
  }


  private static Runnable idleConnectCheck(ProxyRuntime runtime) {
    return () -> {
      MycatReactorThread[] threads = runtime.getMycatReactorThreads();
      for (MycatReactorThread mycatReactorThread : threads) {
        mycatReactorThread.addNIOJob(() -> {
          Thread thread = Thread.currentThread();
          if (thread instanceof MycatReactorThread) {
            MySQLSessionManager manager = ((MycatReactorThread) thread)
                .getMySQLSessionManager();
            manager.idleConnectCheck();
          } else {
            throw new MycatExpection(ReplicaTip.ERROR_EXECUTION_THREAD.getMessage());
          }
        });
      }
    };
  }

  public static void exit() {
    if (runtime != null) {
      runtime.exit();
    }
  }
}
