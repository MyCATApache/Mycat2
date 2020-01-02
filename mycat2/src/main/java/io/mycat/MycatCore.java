/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.buffer.BufferPool;
import io.mycat.buffer.HeapBufferPool;
import io.mycat.calcite.CalciteEnvironment;
import io.mycat.calcite.MetadataManager;
import io.mycat.client.ClientRuntime;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.config.PatternRootConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.PlugRuntime;
import io.mycat.proxy.buffer.ProxyBufferPoolMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOAcceptor;
import io.mycat.proxy.reactor.ReactorThreadManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.util.YamlUtil;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteConnection;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * @author cjw
 **/
public enum MycatCore {
    INSTANCE;
    private ConfigProvider config;
    private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatCore.class);
    private ConcurrentHashMap<String, MySQLDatasource> datasourceMap = new ConcurrentHashMap<>();

    @SneakyThrows
    public void init(ConfigProvider config) {
        this.config = config;


        MycatConfig mycatConfig = config.currentConfig();

        PlugRuntime.INSTCANE.load(mycatConfig);
        JdbcRuntime.INSTANCE.load(mycatConfig);
        ClientRuntime.INSTANCE.load(mycatConfig);
        MetadataManager.INSTANCE.load(mycatConfig);
        ;
        ServerConfig serverConfig = mycatConfig.getServer();

        String bufferPoolText = Optional.ofNullable(mycatConfig.getServer()).map(i -> i.getBufferPool()).map(i -> i.getPoolName()).orElse(HeapBufferPool.class.getName());
        String handlerConstructorText = Optional.ofNullable(mycatConfig.getServer()).map(i -> i.getHandlerName()).orElse(DefaultCommandHandler.class.getName());

        Constructor<?> bufferPoolConstructor = getConstructor(bufferPoolText);
        Constructor<?> handlerConstructor = getConstructor(handlerConstructorText);
        //ProxyBeanProviders proxyBeanProviders = (ProxyBeanProviders)handlerConstructor.newInstance();

        int reactorNumber = serverConfig.getReactorNumber();
        List<MycatReactorThread> list = new ArrayList<>(reactorNumber);
        for (int i = 0; i < reactorNumber; i++) {
            BufferPool bufferPool = (BufferPool) bufferPoolConstructor.newInstance();
            bufferPool.init(mycatConfig.getServer().getBufferPool().getArgs());
            Function<MycatSession, CommandDispatcher> function = new Function<MycatSession, CommandDispatcher>() {
                @Override
                public CommandDispatcher apply(MycatSession session) {
//                    return proxyBeanProviders.createCommandDispatcher();

                    System.out.println();
                    try {
                        CommandDispatcher commandDispatcher = (CommandDispatcher) handlerConstructor.newInstance();
                        commandDispatcher.initRuntime(session);
                        return commandDispatcher;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
            MycatReactorThread thread = new MycatReactorThread(new ProxyBufferPoolMonitor(bufferPool), new MycatSessionManager(function));
            thread.start();
            list.add(thread);
        }
        final ReactorThreadManager reactorManager = new ReactorThreadManager(list);
        NIOAcceptor acceptor = new NIOAcceptor(reactorManager);
        acceptor.startServerChannel(serverConfig.getIp(), serverConfig.getPort());
    }

    public static void main(String[] args) throws Exception {
        PatternRootConfig rootConfig = new PatternRootConfig();
        rootConfig.getHandlers().add(new PatternRootConfig.HandlerToSQLs());
        System.out.println(YamlUtil.dump(rootConfig));
        ConfigProvider bootConfig = RootHelper.INSTCANE.bootConfig(MycatCore.class);
        MycatCore.INSTANCE.init(bootConfig);
        MycatCore.INSTANCE.start();
    }

    private void start() {

    }

    public MySQLDatasource getDatasource(String name) {
        MySQLDatasource datasource2 = datasourceMap.get(name);
        if (datasource2 != null) {
            return datasource2;
        }
        List<DatasourceRootConfig.DatasourceConfig> datasources = config.currentConfig().getDatasource().getDatasources();
        for (DatasourceRootConfig.DatasourceConfig datasourceConfig : datasources) {
            if (name.equals(datasourceConfig.getName())) {
               return datasourceMap.computeIfAbsent(name, s -> new MySQLDatasource(datasourceConfig) {
                });
            }
        }
        return null;
    }

    public void removeDatasource(String name) {
        datasourceMap.remove(name);
    }

    private static Constructor<?> getConstructor(String clazz) throws ClassNotFoundException, NoSuchMethodException {
        Class<?> bufferPoolClass = Class.forName(clazz);
        return bufferPoolClass.getDeclaredConstructor();
    }
//
//    public static void startup(ProxyRuntime rt,
//                               MycatMonitorCallback callback,
//                               AsyncTaskCallBack startFinished)
//            throws IOException {
//        runtime = rt;
//        try {
//            MycatMonitor.setCallback(callback);
//            ReplicaSelectorRuntime.INSTCANE.load();
//            ReplicaHeartbeatRuntime.INSTANCE.load();
//            runtime.startReactor();
//
//            ScheduledExecutorService nonBlockScheduled = Executors.newScheduledThreadPool(1);
//            startMySQLProxyIdleCheckService(nonBlockScheduled);
//            startMySQLProxyHeartbeat(nonBlockScheduled);
//            startMySQLCollectInfoService(nonBlockScheduled);
//
//            runtime.beforeAcceptConnectionProcess();
//            runtime.startAcceptor();
//            startFinished.onFinished(null, null, null);
//        } catch (Exception e) {
//            LOGGER.error("", e);
//            startFinished.onException(e, null, null);
//        }
//    }
//
//    private static void startMySQLProxyHeartbeat(ScheduledExecutorService service) {
//        HeartbeatRootConfig heartbeatRootConfig = runtime
//                .getConfig(ConfigFile.HEARTBEAT);
//        ClusterRootConfig replicasRootConfig = runtime
//                .getConfig(ConfigFile.DATASOURCE);
//        HeartbeatConfig heartbeatConfig = heartbeatRootConfig.getHeartbeat();
//        boolean existUpdate = false;
//        for (ReplicaConfig replica : replicasRootConfig.getReplicas()) {
//            List<DatasourceConfig> datasources = replica.getDatasources();
//            if (datasources != null) {
//                for (DatasourceConfig datasource : datasources) {
//                    if (MycatConfigUtil.isMySQLType(datasource)) {
//                        existUpdate = existUpdate || ReplicaHeartbeatRuntime.INSTANCE
//                                .register(replica, datasource, heartbeatConfig,
//                                        heartBeatStrategy(datasource));
//                    }
//                }
//            }
//        }
//        if (existUpdate) {
//            long period = heartbeatConfig.getReplicaHeartbeatPeriod();
//            service.scheduleAtFixedRate(() -> {
//                        try {
//                            ReplicaHeartbeatRuntime.INSTANCE.heartbeat();
//                        } catch (Exception e) {
//                            LOGGER.error("", e);
//                        }
//                    }, 0, period,
//                    TimeUnit.MILLISECONDS);
//        }
//    }
//
//
//    private static void startMySQLCollectInfoService(ScheduledExecutorService service) {
//        service.scheduleAtFixedRate(() -> {
//            try {
//                ProxyDashboard.INSTANCE.collectInfo(runtime);
//            } catch (Exception e) {
//                LOGGER.error("", e);
//            }
//        }, 0, 1000, TimeUnit.SECONDS);
//    }
//
//    private static void startMySQLProxyIdleCheckService(ScheduledExecutorService service) {
//        HeartbeatRootConfig heartbeatRootConfig = runtime
//                .getConfig(ConfigFile.HEARTBEAT);
//        long idleTimeout = heartbeatRootConfig.getHeartbeat().getIdleTimeout();
//        long replicaIdleCheckPeriod = idleTimeout / 2;
//        service.scheduleAtFixedRate(idleConnectCheck(runtime), replicaIdleCheckPeriod,
//                replicaIdleCheckPeriod,
//                TimeUnit.SECONDS);
//    }
//
//
//    private static Runnable idleConnectCheck(ProxyRuntime runtime) {
//        return () -> {
//            MycatReactorThread[] threads = runtime.getMycatReactorThreads();
//            for (MycatReactorThread mycatReactorThread : threads) {
//                mycatReactorThread.addNIOJob(new NIOJob() {
//                    @Override
//                    public void run(ReactorEnvThread reactor) throws Exception {
//                        Thread thread = Thread.currentThread();
//                        if (thread instanceof MycatReactorThread) {
//                            MySQLSessionManager manager = ((MycatReactorThread) thread)
//                                    .getMySQLSessionManager();
//                            manager.idleConnectCheck();
//                        } else {
//                            throw new MycatException("Replica must running in MycatReactorThread");
//                        }
//                    }
//
//                    @Override
//                    public void stop(ReactorEnvThread reactor, Exception reason) {
//                        LOGGER.error("", reason);
//                    }
//
//                    @Override
//                    public String message() {
//                        return "idleConnectCheck";
//                    }
//                });
//            }
//        };
//    }
//
//    public static void exit() {
//        if (runtime != null) {
//            runtime.exit(new MycatException("normal"));
//        }
//    }
//
//    public static void exit(Exception e) {
//        if (runtime != null) {
//            runtime.exit(e);
//        }
//    }
//
//    private static Consumer<HeartBeatStrategy> heartBeatStrategy(DatasourceConfig datasource) {
//        return heartBeatStrategy -> {
//            if (heartBeatStrategy.isQuit()) {
//                return;
//            }
//            CopyOnWriteArrayList<MycatReactorThread> mycatReactorThreads = runtime.getMycatReactorThreads();
//            MycatReactorThread mycatReactorThread = mycatReactorThreads[ThreadLocalRandom.current()
//                    .nextInt(0, mycatReactorThreads.length)];
//            mycatReactorThread.addNIOJob(new NIOJob() {
//                @Override
//                public void run(ReactorEnvThread reactor) throws Exception {
//                    runtime.getMySQLAPIRuntime().create(datasource.getName(),
//                            new MySQLAPISessionCallback() {
//                                @Override
//                                public void onSession(MySQLAPI mySQLAPI) {
//                                    if (heartBeatStrategy.isQuit()) {
//                                        mySQLAPI.close();
//                                        return;
//                                    }
//                                    OneResultSetCollector collector = new OneResultSetCollector();
//                                    mySQLAPI.query(heartBeatStrategy.getSql(), collector,
//                                            new MySQLAPIExceptionCallback() {
//                                                @Override
//                                                public void onException(Exception exception,
//                                                                        MySQLAPI mySQLAPI) {
//                                                    if (heartBeatStrategy.isQuit()) {
//                                                        return;
//                                                    }
//                                                    heartBeatStrategy.onException(exception);
//                                                }
//
//                                                @Override
//                                                public void onFinished(boolean monopolize, MySQLAPI mySQLAPI) {
//                                                    mySQLAPI.close();
//                                                    List<Map<String, Object>> maps = CollectorUtil.toList(collector);
//                                                    heartBeatStrategy.process(maps);
//                                                }
//
//                                                @Override
//                                                public void onErrorPacket(ErrorPacket errorPacket,
//                                                                          boolean monopolize, MySQLAPI mySQLAPI) {
//                                                    if (heartBeatStrategy.isQuit()) {
//                                                        return;
//                                                    }
//                                                    heartBeatStrategy
//                                                            .onError(errorPacket.getErrorMessageString());
//                                                }
//                                            });
//                                }
//
//                                @Override
//                                public void onException(Exception exception) {
//                                    if (heartBeatStrategy.isQuit()) {
//                                        return;
//                                    }
//                                    heartBeatStrategy.onException(exception);
//                                }
//                            });
//                }
//
//                @Override
//                public void stop(ReactorEnvThread reactor, Exception reason) {
//
//                }
//
//                @Override
//                public String message() {
//                    return null;
//                }
//            });
//
//        };
//    }
}
