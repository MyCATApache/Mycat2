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
import io.mycat.calcite.MetadataManager;
import io.mycat.calcite.MetadataManagerBuilder;
import io.mycat.calcite.SimpleColumnInfo;
import io.mycat.client.ClientRuntime;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.PlugRuntime;
import io.mycat.proxy.buffer.ProxyBufferPoolMonitor;
import io.mycat.proxy.reactor.*;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.YamlUtil;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.mycat.calcite.MetadataManagerBuilder.backEndBuilder;

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
        ReplicaSelectorRuntime.INSTANCE.load(mycatConfig);

        MetadataManager.INSTANCE.load(mycatConfig);
        MetadataManagerBuilder.exampleBuild(MetadataManager.INSTANCE);
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
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            };
            MycatReactorThread thread = new MycatReactorThread(new ProxyBufferPoolMonitor(bufferPool), new MycatSessionManager(function));
            thread.start();
            list.add(thread);
        }

        final ReactorThreadManager reactorManager = new ReactorThreadManager(list);
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
                        return "";
                    }
                });
            }
        },timer.getInitialDelay(),timer.getPeriod(), TimeUnit.valueOf(timer.getTimeUnit()));
        NIOAcceptor acceptor = new NIOAcceptor(reactorManager);
        acceptor.startServerChannel(serverConfig.getIp(), serverConfig.getPort());
    }

    public static void main(String[] args) throws Exception {
        ShardingQueryRootConfig.BackEndTableInfoConfig.BackEndTableInfoConfigBuilder builder = backEndBuilder();
        List<ShardingQueryRootConfig.BackEndTableInfoConfig> tableInfos = Arrays.asList(
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD3").build(),

                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD3").build(),

                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD3").build()
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("partitionCount", "2,1");
        properties.put("partitionLength", "256,512");

        ShardingQueryRootConfig.LogicTableConfig build = ShardingQueryRootConfig.LogicTableConfig.builder()
                .columns(Arrays.asList(ShardingQueryRootConfig.Column.builder()
                        .columnName("id").function(SharingFuntionRootConfig.ShardingFuntion.builder().name("partitionByLong")
                                .clazz("io.mycat.router.function.PartitionByLong").properties(properties).ranges(Collections.emptyMap())
                                .build()).shardingType(SimpleColumnInfo.ShardingType.NATURE_DATABASE_TABLE.name()).build()))
                .createTableSQL("CREATE TABLE `travelrecord` (\n" +
                        "  `id` bigint(20) NOT NULL,\n" +
                        "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int(11) DEFAULT NULL,\n" +
                        "  `blob` longblob DEFAULT NULL,\n" +
                        "  `d` double DEFAULT NULL\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                .build();
        MycatConfig config = new MycatConfig() ;
        ShardingQueryRootConfig.LogicSchemaConfig logicSchemaConfig = new ShardingQueryRootConfig.LogicSchemaConfig();
        config.getMetadata().getSchemas().put("a",logicSchemaConfig);
        logicSchemaConfig.setTables(Collections.singletonMap("1",build));
        System.out.println(Arrays.asList());
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
        throw new IllegalArgumentException();
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
//                                    mySQLAPI.query(heartBeatStrategy.getCommand(), collector,
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
