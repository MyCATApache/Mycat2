/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is open software: you can redistribute it and/or modify it under the terms of the
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

import com.atomikos.icatch.jta.UserTransactionImp;
import io.mycat.api.MySQLAPI;
import io.mycat.api.callback.MySQLAPIExceptionCallback;
import io.mycat.api.collector.CollectorUtil;
import io.mycat.api.collector.OneResultSetCollector;
import io.mycat.beans.MySQLDatasource;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.HeapBufferPool;
import io.mycat.client.InterceptorRuntime;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.transactionSession.JTATransactionSession;
import io.mycat.ext.MySQLAPIImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.metadata.MetadataManager;
import io.mycat.plug.PlugRuntime;
import io.mycat.proxy.buffer.ProxyBufferPoolMonitor;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.reactor.*;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.runtime.MycatDataContextSupport;
import io.mycat.runtime.ProxyTransactionSession;
import io.mycat.util.CharsetUtil;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

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

        ReplicaSelectorRuntime.INSTANCE.load(mycatConfig);
        JdbcRuntime.INSTANCE.load(mycatConfig);
        InterceptorRuntime.INSTANCE.load(mycatConfig);


        MetadataManager.INSTANCE.load(mycatConfig);

        CharsetUtil.init(null);
        startProxy(mycatConfig);
    }

    private void startProxy(MycatConfig mycatConfig) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, IOException, InterruptedException {
        ServerConfig serverConfig = mycatConfig.getServer();

        String bufferPoolText = Optional.ofNullable(mycatConfig.getServer()).map(i -> i.getBufferPool()).map(i -> i.getPoolName()).orElse(HeapBufferPool.class.getName());
        String handlerConstructorText = Optional.ofNullable(mycatConfig.getServer()).map(i -> i.getHandlerName()).orElse(DefaultCommandHandler.class.getName());

        Constructor<?> bufferPoolConstructor = getConstructor(bufferPoolText);
        Constructor<?> handlerConstructor = getConstructor(handlerConstructorText);

        int reactorNumber = serverConfig.getReactorNumber();
        List<MycatReactorThread> list = new ArrayList<>(reactorNumber);
        for (int i = 0; i < reactorNumber; i++) {
            BufferPool bufferPool = (BufferPool) bufferPoolConstructor.newInstance();
            bufferPool.init(mycatConfig.getServer().getBufferPool().getArgs());
            Function<MycatSession, CommandDispatcher> function = session -> {
                try {
                    CommandDispatcher commandDispatcher = (CommandDispatcher) handlerConstructor.newInstance();
                    commandDispatcher.initRuntime(session);
                    return commandDispatcher;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            Map<String, PatternRootConfig.UserConfig> userConfigMap = mycatConfig.getInterceptors().stream().map(u -> u.getUser()).collect((Collectors.toMap(k -> k.getUsername(), v -> v)));
            MycatReactorThread thread = new MycatReactorThread(new ProxyBufferPoolMonitor(bufferPool), new MycatSessionManager(function, new AuthenticatorImpl(userConfigMap)));
            thread.start();
            list.add(thread);
        }

        final ReactorThreadManager reactorManager = new ReactorThreadManager(list);
        idleConnectCheck(mycatConfig, reactorManager);
        heartbeat(mycatConfig, reactorManager);

        TimerConfig timer = mycatConfig.getCluster().getTimer();
        NIOAcceptor acceptor = new NIOAcceptor(reactorManager);


        HashMap<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap = new HashMap<>();


        transcationFactoryMap.put(TransactionType.JDBC_TRANSACTION_TYPE, mycatDataContext -> new JTATransactionSession(mycatDataContext, () -> new UserTransactionImp()));
        transcationFactoryMap.put(TransactionType.PROXY_TRANSACTION_TYPE, mycatDataContext -> new ProxyTransactionSession(mycatDataContext));

        MycatDataContextSupport.INSTANCE.init(mycatConfig.getServer().getWorker(), transcationFactoryMap);


        long wait = TimeUnit.valueOf(timer.getTimeUnit()).toMillis(timer.getInitialDelay()) + TimeUnit.SECONDS.toMillis(1);
        Thread.sleep(wait);
        acceptor.startServerChannel(serverConfig.getIp(), serverConfig.getPort());
        initFrontSessionChecker(mycatConfig, reactorManager);

        LOGGER.info("mycat starts successful");
    }

    private void initFrontSessionChecker(MycatConfig mycatConfig, ReactorThreadManager reactorManager) {
        TimerConfig frontSessionChecker = mycatConfig.getServer().getTimer();
        if (frontSessionChecker.getPeriod() > 0) {
            ScheduleUtil.getTimer().scheduleAtFixedRate(() -> {
                try {
                    for (MycatReactorThread thread : reactorManager.getList()) {
                        thread.addNIOJob(new NIOJob() {
                            @Override
                            public void run(ReactorEnvThread reactor) throws Exception {
                                thread.getFrontManager().check();
                            }

                            @Override
                            public void stop(ReactorEnvThread reactor, Exception reason) {

                            }

                            @Override
                            public String message() {
                                return "frontSessionChecker";
                            }
                        });
                    }
                } catch (Exception e) {
                    LOGGER.error("{}", e);
                }
            }, frontSessionChecker.getInitialDelay(), frontSessionChecker.getPeriod(), TimeUnit.valueOf(frontSessionChecker.getTimeUnit()));
        }
    }

    private void idleConnectCheck(MycatConfig mycatConfig, ReactorThreadManager reactorManager) {
        TimerConfig timer = mycatConfig.getDatasource().getTimer();
        ScheduleUtil.getTimer().scheduleAtFixedRate(() -> {
            for (MycatReactorThread thread : reactorManager.getList()) {
                thread.addNIOJob(new NIOJob() {
                    @Override
                    public void run(ReactorEnvThread reactor) throws Exception {
                        thread.getMySQLSessionManager().idleConnectCheck();
                    }

                    @Override
                    public void stop(ReactorEnvThread reactor, Exception reason) {

                    }

                    @Override
                    public String message() {
                        return "idleConnectCheck";
                    }
                });
            }
        }, timer.getInitialDelay(), timer.getPeriod(), TimeUnit.valueOf(timer.getTimeUnit()));
    }

    private void heartbeat(MycatConfig mycatConfig, ReactorThreadManager reactorManager) {
        for (ClusterRootConfig.ClusterConfig cluster : mycatConfig.getCluster().getClusters()) {
            if ("mysql".equalsIgnoreCase(cluster.getHeartbeat().getReuqestType())) {
                String replicaName = cluster.getName();
                for (String datasource : cluster.getMasters())
                    ReplicaSelectorRuntime.INSTANCE.putHeartFlow(replicaName, datasource, heartBeatStrategy -> reactorManager.getRandomReactor().addNIOJob(new NIOJob() {
                        @Override
                        public void run(ReactorEnvThread reactor) throws Exception {
                            MySQLTaskUtil.getMySQLSessionForTryConnect(datasource, new SessionCallBack<MySQLClientSession>() {
                                @Override
                                public void onSession(MySQLClientSession session, Object sender, Object attr) {
                                    MySQLAPIImpl mySQLAPI = new MySQLAPIImpl(session);
                                    OneResultSetCollector objects = new OneResultSetCollector();
                                    mySQLAPI.query(heartBeatStrategy.getSql(), objects, new MySQLAPIExceptionCallback() {
                                        @Override
                                        public void onException(Exception exception, @NonNull MySQLAPI mySQLAPI) {
                                            heartBeatStrategy.onException(exception);
                                        }

                                        @Override
                                        public void onFinished(boolean monopolize, @NonNull MySQLAPI mySQLAPI) {
                                            try {
                                                List<Map<String, Object>> maps = CollectorUtil.toList(objects);
                                                LOGGER.debug("proxy heartbeat {}", Objects.toString(maps));
                                                heartBeatStrategy.process(maps);
                                            } finally {
                                                mySQLAPI.close();
                                            }
                                        }

                                        @Override
                                        public void onErrorPacket(@NonNull ErrorPacket errorPacket, boolean monopolize, @NonNull MySQLAPI mySQLAPI) {
                                            mySQLAPI.close();
                                            heartBeatStrategy.onError(errorPacket.getErrorMessageString());
                                        }
                                    });
                                }

                                @Override
                                public void onException(Exception exception, Object sender, Object attr) {
                                    heartBeatStrategy.onException(exception);
                                }
                            });
                        }

                        @Override
                        public void stop(ReactorEnvThread reactor, Exception reason) {

                        }

                        @Override
                        public String message() {
                            return "heartbeat";
                        }
                    }));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(MycatCore.class);
        MycatCore.INSTANCE.init(bootConfig);
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
        throw new IllegalArgumentException();
    }

    public void removeDatasource(String name) {
        datasourceMap.remove(name);
    }

    private static Constructor<?> getConstructor(String clazz) throws ClassNotFoundException, NoSuchMethodException {
        Class<?> bufferPoolClass = Class.forName(clazz);
        return bufferPoolClass.getDeclaredConstructor();
    }
}
