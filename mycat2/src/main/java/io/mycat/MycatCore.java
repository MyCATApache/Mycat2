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

import io.mycat.api.MySQLAPI;
import io.mycat.api.callback.MySQLAPIExceptionCallback;
import io.mycat.api.callback.MySQLAPISessionCallback;
import io.mycat.api.collector.CollectorUtil;
import io.mycat.api.collector.OneResultSetCollector;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.config.ConfigFile;
import io.mycat.config.MycatConfigUtil;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.callback.EmptyAsyncTaskCallBack;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;
import io.mycat.proxy.monitor.ProxyDashboard;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.ReplicaHeartbeatRuntime;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author cjw
 **/
public class MycatCore {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatCore.class);

  private static ProxyRuntime runtime;

  public static void main(String[] args) throws Exception {
    runtime = new ProxyRuntime(ConfigRuntime.INSTCANE.load());
    startup(runtime, new MycatMonitorLogCallback(), EmptyAsyncTaskCallBack.INSTANCE);
  }

  public static void startup(ProxyRuntime rt,
      MycatMonitorCallback callback,
      AsyncTaskCallBack startFinished)
      throws IOException {
    runtime = rt;
    try {
      MycatMonitor.setCallback(callback);
      ReplicaSelectorRuntime.INSTCANE.load();
      ReplicaHeartbeatRuntime.INSTANCE.load();
      runtime.startReactor();

      ScheduledExecutorService nonBlockScheduled = Executors.newScheduledThreadPool(1);
      startMySQLProxyIdleCheckService(nonBlockScheduled);
      startMySQLProxyHeartbeat(nonBlockScheduled);
      startMySQLCollectInfoService(nonBlockScheduled);

      runtime.beforeAcceptConnectionProcess();
      runtime.startAcceptor();
      startFinished.onFinished(null, null, null);
    } catch (Exception e) {
      LOGGER.error("", e);
      startFinished.onException(e, null, null);
    }
  }

  private static void startMySQLProxyHeartbeat(ScheduledExecutorService service) {
    HeartbeatRootConfig heartbeatRootConfig = runtime
        .getConfig(ConfigFile.HEARTBEAT);
    ReplicasRootConfig replicasRootConfig = runtime
        .getConfig(ConfigFile.DATASOURCE);
    HeartbeatConfig heartbeatConfig = heartbeatRootConfig.getHeartbeat();
    for (ReplicaConfig replica : replicasRootConfig.getReplicas()) {
      List<DatasourceConfig> datasources = replica.getDatasources();
      if (datasources != null) {
        for (DatasourceConfig datasource : datasources) {
          if (MycatConfigUtil.isMySQLType(datasource)) {
            ReplicaHeartbeatRuntime.INSTANCE.register(replica, datasource, heartbeatConfig,
                heartBeatStrategy(datasource));
          }
        }
      }
    }
    long period = heartbeatConfig.getReplicaHeartbeatPeriod();
    service.scheduleAtFixedRate(() -> {
      ReplicaHeartbeatRuntime.INSTANCE.heartbeat();
      Collection<MySQLDataSourceEx> datasourceList = runtime.getMySQLDatasourceList();
      for (MySQLDataSourceEx datasource : datasourceList) {
        if (datasource != null) {
          datasource.heartBeat();
        }
      }
    }, 0, period, TimeUnit.SECONDS);
  }


  private static void startMySQLCollectInfoService(ScheduledExecutorService service) {
    service.scheduleAtFixedRate(() -> {
      try {
        ProxyDashboard.INSTANCE.collectInfo(runtime);
      } catch (Exception e) {
        LOGGER.error("", e);
      }
    }, 0, 5, TimeUnit.MINUTES);
  }

  private static void startMySQLProxyIdleCheckService(ScheduledExecutorService service) {
    HeartbeatRootConfig heartbeatRootConfig = runtime
        .getConfig(ConfigFile.HEARTBEAT);
    long idleTimeout = heartbeatRootConfig.getHeartbeat().getIdleTimeout();
    long replicaIdleCheckPeriod = idleTimeout / 2;
    service.scheduleAtFixedRate(idleConnectCheck(runtime), replicaIdleCheckPeriod,
        replicaIdleCheckPeriod,
        TimeUnit.SECONDS);
  }


  private static Runnable idleConnectCheck(ProxyRuntime runtime) {
    return () -> {
      MycatReactorThread[] threads = runtime.getMycatReactorThreads();
      for (MycatReactorThread mycatReactorThread : threads) {
        mycatReactorThread.addNIOJob(new NIOJob() {
          @Override
          public void run(ReactorEnvThread reactor) throws Exception {
            Thread thread = Thread.currentThread();
            if (thread instanceof MycatReactorThread) {
              MySQLSessionManager manager = ((MycatReactorThread) thread)
                  .getMySQLSessionManager();
              manager.idleConnectCheck();
            } else {
              throw new MycatException("Replica must running in MycatReactorThread");
            }
          }

          @Override
          public void stop(ReactorEnvThread reactor, Exception reason) {
            LOGGER.error("", reason);
          }

          @Override
          public String message() {
            return "idleConnectCheck";
          }
        });
      }
    };
  }

  public static void exit() {
    if (runtime != null) {
      runtime.exit(new MycatException("normal"));
    }
  }

  public static void exit(Exception e) {
    if (runtime != null) {
      runtime.exit(e);
    }
  }

  private static Consumer<HeartBeatStrategy> heartBeatStrategy(DatasourceConfig datasource) {
    return heartBeatStrategy -> {
      if (heartBeatStrategy.isQuit()) {
        return;
      }
      runtime.getMySQLAPIRuntime().create(datasource.getName(),
          new MySQLAPISessionCallback() {
            @Override
            public void onSession(MySQLAPI mySQLAPI) {
              if (heartBeatStrategy.isQuit()) {
                mySQLAPI.close();
                return;
              }
              OneResultSetCollector collector = new OneResultSetCollector();
              mySQLAPI.query(heartBeatStrategy.getSql(), collector,
                  new MySQLAPIExceptionCallback() {
                    @Override
                    public void onException(Exception exception,
                        MySQLAPI mySQLAPI) {
                      if (heartBeatStrategy.isQuit()) {
                        return;
                      }
                      heartBeatStrategy.onException(exception);
                    }

                    @Override
                    public void onFinished(boolean monopolize, MySQLAPI mySQLAPI) {
                      mySQLAPI.close();
                      heartBeatStrategy.process(CollectorUtil.toList(collector));
                    }

                    @Override
                    public void onErrorPacket(ErrorPacket errorPacket,
                        boolean monopolize, MySQLAPI mySQLAPI) {
                      if (heartBeatStrategy.isQuit()) {
                        return;
                      }
                      heartBeatStrategy
                          .onError(errorPacket.getErrorMessageString());
                    }
                  });
            }

            @Override
            public void onException(Exception exception) {
              if (heartBeatStrategy.isQuit()) {
                return;
              }
              heartBeatStrategy.onException(exception);
            }
          });
    };
  }
}
