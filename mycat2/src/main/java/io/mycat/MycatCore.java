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
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.logTip.ReplicaTip;
import io.mycat.proxy.monitor.ProxyDashboard;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import java.io.IOException;
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

  public static void main(String[] args) throws Exception {
    String resourcesPath = System.getProperty("MYCAT_HOME");
    LOGGER.info("MYCAT_HOME:{}", resourcesPath);
    if (resourcesPath == null) {
      resourcesPath = ProxyRuntime.getResourcesPath(MycatCore.class);
    }
    startup(resourcesPath, MycatProxyBeanProviders.INSTANCE, new MycatMonitorLogCallback(),
        new AsyncTaskCallBack() {
          @Override
          public void onFinished(Object sender, Object result, Object attr) {

          }

          @Override
          public void onException(Exception e, Object sender, Object attr) {

          }

        });
    return;

  }

  public static void startup(String resourcesPath, ProxyBeanProviders proxyBeanProviders,
      MycatMonitorCallback callback,
      AsyncTaskCallBack startFinished)
      throws IOException {
    ProxyRuntime runtime = ProxyRuntime.INSTANCE;
    MycatMonitor.setCallback(callback);
    runtime.initCharset(resourcesPath);
    runtime.loadProxy(resourcesPath);
    runtime.loadMycat(resourcesPath);
    runtime.initPlug();
    MycatRouterConfig routerConfig = runtime.initRouterConfig(resourcesPath);
    MycatRouter router = new MycatRouter(routerConfig);
    runtime.initReactor(proxyBeanProviders, new AsyncTaskCallBack() {
      @Override
      public void onFinished(Object sender, Object result, Object attr) {
        runtime.initRepliac(proxyBeanProviders, new AsyncTaskCallBack() {
          @Override
          public void onFinished(Object sender, Object result, Object attr) {
            try {
              runtime.initDataNode();
              runtime.initSecurityManager();
              runtime.initAcceptor();
              ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
              HeartbeatRootConfig heartbeatRootConfig = ProxyRuntime.INSTANCE
                                                            .getConfig(ConfigEnum.HEARTBEAT);

              long idleTimeout = heartbeatRootConfig.getHeartbeat().getIdleTimeout();
              long replicaIdleCheckPeriod = idleTimeout / 2;
              service.scheduleAtFixedRate(idleConnectCheck(), 0, replicaIdleCheckPeriod, TimeUnit.SECONDS);
                long period = heartbeatRootConfig.getHeartbeat().getReplicaHeartbeatPeriod();
//              service.scheduleAtFixedRate(new Runnable() {
//                @Override
//                public void run() {
//                  Collection<MySQLDataSourceEx> datasourceList = runtime.getMySQLDatasourceList();
//                  for (MySQLDataSourceEx datasource : datasourceList) {
//                    datasource.heartBeat();
//                  }
//                }
//              }, 0, period, TimeUnit.SECONDS);
              service.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {

                  try {
                    ProxyDashboard.INSTANCE.collectInfo();
                  }catch (Exception e){
                    e.printStackTrace();
                  }

                }
              }, 0, 5, TimeUnit.SECONDS);

              startFinished.onFinished(null, null, null);

            } catch (Exception e) {
              e.printStackTrace();
              startFinished.onException(e, null, null);
            }
          }

          @Override
          public void onException(Exception e, Object sender, Object attr) {

          }


        });
      }

      @Override
      public void onException(Exception e, Object sender, Object attr) {

      }
    });


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


  private static Runnable idleConnectCheck() {
    return () -> {
      MycatReactorThread[] threads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
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
}
