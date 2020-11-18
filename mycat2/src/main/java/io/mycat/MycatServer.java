package io.mycat;

import io.mycat.beans.MySQLDatasource;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.DefaultReactorBufferPool;
import io.mycat.buffer.HeapBufferPool;
import io.mycat.buffer.ReactorBufferPool;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasourceprovider.AtomikosDatasourceProvider;
import io.mycat.datasource.jdbc.datasourceprovider.DruidDatasourceProvider;
import io.mycat.manager.ManagerCommandDispatcher;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.proxy.buffer.ProxyBufferPoolMonitor;
import io.mycat.proxy.reactor.*;
import io.mycat.proxy.session.*;
import io.mycat.runtime.LocalTransactionSession;
import io.mycat.runtime.ProxyTransactionSession;
import io.mycat.thread.SimpleMycatContextBindingThreadPool;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
public class MycatServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatServer.class);

    private final ConcurrentHashMap<String, MySQLDatasource> datasourceMap = new ConcurrentHashMap<>();

    private final MycatServerConfig serverConfig;

    private ReactorThreadManager reactorManager;

    private ReactorThreadManager managerManager;

    private final MycatContextThreadPool mycatContextThreadPool;

    private final Map<TransactionType, Function<MycatDataContext, TransactionSession>> transcationFactoryMap;

    private final LoadBalanceManager loadBalanceManager;

    private final MycatWorkerProcessor mycatWorkerProcessor;

    private final DatasourceConfigProvider datasourceConfigProvider;

    private final Authenticator authenticator;

    @SneakyThrows
    public MycatServer(MycatServerConfig serverConfig,
                       Authenticator refAuthenticator,
                       DatasourceConfigProvider datasourceConfigProvider) {
        this.serverConfig = serverConfig;
        this.authenticator = refAuthenticator;
        this.loadBalanceManager = new LoadBalanceManager(serverConfig.getLoadBalance());
        this.datasourceConfigProvider = datasourceConfigProvider;
        io.mycat.config.ServerConfig serverConfigServer = serverConfig.getServer();
        ThreadPoolExecutorConfig workerPool = serverConfigServer.getWorkerPool();
        this.mycatWorkerProcessor = new MycatWorkerProcessor(workerPool, serverConfigServer.getTimeWorkerPool());
        this.transcationFactoryMap = new HashMap<>();
        this.transcationFactoryMap.put(TransactionType.PROXY_TRANSACTION_TYPE, mycatDataContext -> new ProxyTransactionSession(mycatDataContext));
        this.transcationFactoryMap.put(TransactionType.JDBC_TRANSACTION_TYPE, mycatDataContext -> new LocalTransactionSession(mycatDataContext));
        this.mycatContextThreadPool = new MycatContextThreadPoolImpl(
                mycatWorkerProcessor.getMycatWorker(),
                workerPool.getTaskTimeout(),
                TimeUnit.valueOf(workerPool.getTimeUnit()));
    }

    @SneakyThrows
    public void start() {
        startProxy(this.serverConfig.getServer());
        startManager(this.serverConfig.getManager());
    }


    private void startManager(ManagerConfig manager) throws IOException {
        if (manager == null) {
            return;
        }
        List<UserConfig> users = manager.getUsers();
        if (users == null || users.isEmpty()) {
            return;
        }
        List<MycatReactorThread> list = new ArrayList<>(1);
        Function<MycatSession, CommandDispatcher> function = session -> {
            try {
                CommandDispatcher commandDispatcher = new ManagerCommandDispatcher();
                commandDispatcher.initRuntime(session);
                return commandDispatcher;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        int bufferPoolPageSize = 1024 * 1024 * 2;
        short bufferPoolChunkSize = 8192;
        short bufferPoolPageNumber = (short) (Runtime.getRuntime().maxMemory() * 0.8 / bufferPoolPageSize);
        Map<String, UserConfig> userConfigMap = users.stream().collect((Collectors.toMap(k -> k.getUsername(), v -> v)));

        Map<String, Object> bufferConfig = new HashMap<>();
        bufferConfig.put("pageSize", bufferPoolPageSize);
        bufferConfig.put("chunkSize", bufferPoolChunkSize);
        bufferConfig.put("pageCount", bufferPoolPageNumber);

        MycatReactorThread thread = new MycatReactorThread(new DefaultReactorBufferPool(bufferConfig), new MycatSessionManager(function,
                new AuthenticatorImpl(userConfigMap),
                this.transcationFactoryMap,
                this.mycatContextThreadPool));
        thread.start();
        list.add(thread);
        managerManager = new ReactorThreadManager(list);
        NIOAcceptor acceptor = new NIOAcceptor(managerManager);
        acceptor.startServerChannel(manager.getIp(), manager.getPort());
    }

    private void startProxy(io.mycat.config.ServerConfig serverConfig) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, IOException, InterruptedException {


        String handlerConstructorText = (DefaultCommandHandler.class.getName());


        DefaultReactorBufferPool defaultReactorBufferPool = new DefaultReactorBufferPool(Optional
                .ofNullable(serverConfig).map(i -> i.getBufferPool()).map(i -> i.getArgs()).orElse(BufferPoolConfig.defaultValue()));

        Constructor<?> handlerConstructor = getConstructor(handlerConstructorText);

        int reactorNumber = Optional.ofNullable(serverConfig).map(i -> i.getReactorNumber()).orElse(1);
        List<MycatReactorThread> list = new ArrayList<>(reactorNumber);
        for (int i = 0; i < reactorNumber; i++) {
            Function<MycatSession, CommandDispatcher> function = session -> {
                try {
                    CommandDispatcher commandDispatcher = (CommandDispatcher) handlerConstructor.newInstance();
                    commandDispatcher.initRuntime(session);
                    return commandDispatcher;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            MycatReactorThread thread = new MycatReactorThread(defaultReactorBufferPool, new MycatSessionManager(function, authenticator, transcationFactoryMap, mycatContextThreadPool));
            thread.start();
            list.add(thread);
        }

        this.reactorManager = new ReactorThreadManager(list);
        idleConnectCheck(serverConfig.getIdleTimer(), reactorManager);

        NIOAcceptor acceptor = new NIOAcceptor(reactorManager);

        acceptor.startServerChannel(serverConfig.getIp(), serverConfig.getPort());
        initFrontSessionChecker(serverConfig.getIdleTimer(), reactorManager);

        LOGGER.info("mycat starts successful");
    }

    private void initFrontSessionChecker(TimerConfig frontSessionChecker, ReactorThreadManager reactorManager) {
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
                    LOGGER.error("", e);
                }
            }, frontSessionChecker.getInitialDelay(), frontSessionChecker.getPeriod(), TimeUnit.valueOf(frontSessionChecker.getTimeUnit()));
        }
    }

    private void idleConnectCheck(TimerConfig timer, ReactorThreadManager reactorManager) {
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

    public MySQLDatasource getDatasource(String name) {
        MySQLDatasource datasource = datasourceMap.get(name);
        if (datasource != null) {
            return datasource;
        }
        DatasourceConfig datasourceConfig = Objects.requireNonNull(datasourceConfigProvider.get()).get(name);
        if (datasourceConfig != null && datasourceConfig.computeType().isNative()) {
            return datasourceMap.computeIfAbsent(name, s -> {
                MySQLDatasource mySQLDatasource = new MySQLDatasource(datasourceConfig) {
                    @Override
                    public boolean isValid() {
                        MySQLDatasource datasource = datasourceMap.get(this.getName());
                        if (datasource != null && this == datasource) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                };
                return mySQLDatasource;
            });
        }
        return null;
    }


    private static Constructor<?> getConstructor(String clazz) throws ClassNotFoundException, NoSuchMethodException {
        Class<?> bufferPoolClass = Class.forName(clazz);
        return bufferPoolClass.getDeclaredConstructor();
    }

    private static void runExtra(List<String> objectMap) {
        for (String clazz : Optional.ofNullable(objectMap)
                .orElse(Collections.emptyList())) {
            try {
                Class<?> aClass = Class.forName(clazz);
                Constructor<?> declaredConstructor = aClass.getDeclaredConstructors()[0];
                Runnable o = (Runnable) declaredConstructor.newInstance();
                o.run();
            } catch (Throwable e) {
                LOGGER.error("can not run:{}", clazz, e);
            }
        }
    }
}