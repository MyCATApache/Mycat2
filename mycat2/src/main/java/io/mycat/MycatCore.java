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
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author cjw
 **/
public class MycatCore {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatCore.class);

  private static ProxyRuntime runtime;

  public static void main(String[] args) throws Exception {
    String configResourceKeyName = "MYCAT_HOME";
    String resourcesPath = System.getProperty(configResourceKeyName);
    if (resourcesPath == null) {
      resourcesPath = Paths.get("").toAbsolutePath().toString();
    }
    LOGGER.info("config folder path:{}", resourcesPath);
    LOGGER.info(configResourceKeyName, resourcesPath);
    if (resourcesPath == null || Boolean.getBoolean("DEBUG")) {
      resourcesPath = ProxyRuntime.getResourcesPath(MycatCore.class);
    }
    runtime = new ProxyRuntime(
        ConfigLoader.load(resourcesPath, GlobalConfig.genVersion()));
    startup(resourcesPath, runtime, new MycatMonitorLogCallback(), EmptyAsyncTaskCallBack.INSTANCE);
    return;

  }

  public static void startup(String resourcesPath, ProxyRuntime rt,
      MycatMonitorCallback callback,
      AsyncTaskCallBack startFinished)
      throws IOException {
    runtime = rt;
    try {
      MycatMonitor.setCallback(callback);
      runtime.startReactor();

      ScheduledExecutorService nonBlockScheduled = Executors.newScheduledThreadPool(1);
      HeartbeatRootConfig heartbeatRootConfig = runtime
          .getConfig(ConfigEnum.HEARTBEAT);
      startMySQLProxyIdleCheckService(nonBlockScheduled, heartbeatRootConfig);
      startMySQLProxyHeartbeat(nonBlockScheduled, heartbeatRootConfig);
      startMySQLCollectInfoService(nonBlockScheduled);

      runtime.beforeAcceptConnectionProcess();
      runtime.startAcceptor();
      startFinished.onFinished(null, null, null);
    } catch (Exception e) {
      LOGGER.error("",e);
      startFinished.onException(e, null, null);
    }
  }

  private static void startMySQLProxyHeartbeat(ScheduledExecutorService service,
      HeartbeatRootConfig heartbeatRootConfig) {
    long period = heartbeatRootConfig.getHeartbeat().getReplicaHeartbeatPeriod();
    service.scheduleAtFixedRate(() -> {
      Collection<MySQLDataSourceEx> datasourceList = runtime.getMySQLDatasourceList();
      for (MySQLDataSourceEx datasource : datasourceList) {
        datasource.heartBeat();
      }
    }, 0, period, TimeUnit.SECONDS);
  }

  private static void startMySQLCollectInfoService(ScheduledExecutorService service) {
    service.scheduleAtFixedRate(() -> {
      try {
        ProxyDashboard.INSTANCE.collectInfo(runtime);
      } catch (Exception e) {
        LOGGER.error("",e);
      }
    }, 0, 5, TimeUnit.MINUTES);
  }

  private static void startMySQLProxyIdleCheckService(ScheduledExecutorService service,
      HeartbeatRootConfig heartbeatRootConfig) {
    long idleTimeout = heartbeatRootConfig.getHeartbeat().getIdleTimeout();
    long replicaIdleCheckPeriod = idleTimeout / 2;
    service.scheduleAtFixedRate(idleConnectCheck(runtime), 0, replicaIdleCheckPeriod,
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
}
