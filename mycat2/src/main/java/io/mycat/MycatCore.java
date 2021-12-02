/**
 * Copyright (C) <2021>  <chen junwen>
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

import io.mycat.beans.mysql.MySQLVersion;
import io.mycat.calcite.ExecutorProvider;
import io.mycat.config.*;
import io.mycat.executor.ExecutorProviderImpl;
import io.mycat.exporter.SqlRecorderRuntime;
import io.mycat.monitor.MycatSQLLogMonitor;
import io.mycat.monitor.MycatSQLLogMonitorImpl;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.config.FileStorageManagerImpl;
import io.mycat.sqlhandler.config.StdStorageManagerImpl;
import io.mycat.sqlhandler.config.StorageManager;
import io.mycat.vertx.VertxMycatServer;
import io.mycat.vertxmycat.MycatVertxMetricsFactory;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import lombok.SneakyThrows;
import org.apache.calcite.util.RxBuiltInMethod;
import org.apache.groovy.util.Maps;
import org.apache.zookeeper.client.ConnectStringParser;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author cjw
 **/
public class MycatCore {
    final static Logger logger = LoggerFactory.getLogger(MycatCore.class);
    public static final String PROPERTY_MODE_LOCAL = "local";
    public static final String PROPERTY_MODE_CLUSTER = "cluster";
    public static final String PROPERTY_METADATADIR = "metadata";
    private final MycatServer mycatServer;
    private final Path baseDirectory;

    static {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = MycatCore.class.getClassLoader();
        }
        boolean initialize = !Boolean.getBoolean("MYCAT_LAZY_STARTUP");
        try {
            Class.forName("com.alibaba.druid.sql.visitor.SQLASTOutputVisitor", initialize, classLoader);
            Class.forName("com.alibaba.druid.sql.parser.SQLExprParser", initialize, classLoader);
            Class.forName("org.apache.calcite.rel.core.Project", initialize, classLoader);
            Class.forName("oshi.util.platform.windows.PerfCounterQuery", initialize, classLoader);
            Class.forName("io.mycat.datasource.jdbc.datasource.JdbcConnectionManager", initialize, classLoader);
            Class.forName("org.apache.calcite.sql.SqlUtil", initialize, classLoader);
            Class.forName("org.apache.calcite.plan.RelOptUtil", initialize, classLoader);
            Class.forName("org.apache.calcite.mycat.MycatBuiltInMethod", initialize, classLoader);
            Class.forName("com.alibaba.druid.sql.SQLUtils", initialize, classLoader);
        } catch (ClassNotFoundException e) {
            throw new Error("init error. " + e.toString());
        }
    }


    @SneakyThrows
    public MycatCore() {
        RxBuiltInMethod[] values = RxBuiltInMethod.values();
        // TimeZone.setDefault(ZoneInfo.getTimeZone("UTC"));
        String path = findMycatHome();
        boolean enableGSI = false;
        this.baseDirectory = Paths.get(path).toAbsolutePath();
        System.out.println("path:" + this.baseDirectory);
        ServerConfiguration serverConfiguration = new ServerConfigurationImpl(MycatCore.class, path);
        MycatServerConfig serverConfig = serverConfiguration.serverConfig();
        MetaClusterCurrent.register(Maps.of(MycatServerConfig.class, serverConfig, serverConfig.getServer().getClass(), serverConfig.getServer()));
        MySQLVersion.setServerVersion(serverConfig.getServer().getServerVersion());
        String datasourceProvider = Optional.ofNullable(serverConfig.getDatasourceProvider()).orElse(io.mycat.datasource.jdbc.DruidDatasourceProvider.class.getCanonicalName());
        ThreadPoolExecutorConfig workerPool = serverConfig.getServer().getWorkerPool();


        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setWorkerPoolSize(workerPool.getMaxPoolSize());
        vertxOptions.setMaxWorkerExecuteTime(workerPool.getTaskTimeout());
        vertxOptions.setMaxWorkerExecuteTimeUnit(TimeUnit.valueOf(workerPool.getTimeUnit()));
        vertxOptions.setEventLoopPoolSize(serverConfig.getServer().getReactorNumber());
        vertxOptions.getMetricsOptions().setEnabled(true);
        vertxOptions.getMetricsOptions().setFactory(new MycatVertxMetricsFactory());
        this.mycatServer = newMycatServer(serverConfig);

        HashMap<Class, Object> context = new HashMap<>();
        Vertx vertx = Vertx.vertx(vertxOptions);
        context.put(IOExecutor.class, IOExecutor.DEFAULT);
        context.put(serverConfig.getServer().getClass(), serverConfig.getServer());
        context.put(serverConfiguration.getClass(), serverConfiguration);
        context.put(serverConfig.getClass(), serverConfig);
        context.put(LoadBalanceManager.class, new LoadBalanceManager(serverConfig.getLoadBalance()));
        context.put(Vertx.class, vertx);
        context.put(this.mycatServer.getClass(), mycatServer);
        context.put(MycatServer.class, mycatServer);
        context.put(SqlRecorderRuntime.class, SqlRecorderRuntime.INSTANCE);
        context.put(LockService.class, new LocalLockServiceImpl());
        context.put(MycatSQLLogMonitor.class, new MycatSQLLogMonitorImpl(serverConfig.getServer().getMycatId(), serverConfig.getMonitor(), vertx));
        context.put(ExecutorProvider.class, ExecutorProviderImpl.INSTANCE);
        ////////////////////////////////////////////tmp///////////////////////////////////
        MetaClusterCurrent.register(context);

        String mode = Optional.ofNullable(System.getProperty("mode"))
                .orElse(serverConfig.getMode());


        boolean initConfig = false;
        if (!Files.exists(this.baseDirectory)) {
            Files.createDirectory(this.baseDirectory);
            initConfig = true;
        }
        if (Files.list(this.baseDirectory).noneMatch(Files::isDirectory)) {
            initConfig = true;
        }

        FileStorageManagerImpl fileStorageManager = new FileStorageManagerImpl(this.baseDirectory);
        StdStorageManagerImpl storageManager = new StdStorageManagerImpl(fileStorageManager);

        Arrays.asList(LogicSchemaConfig.class,
                ClusterConfig.class,
                DatasourceConfig.class,
                UserConfig.class,
                SequenceConfig.class,
                SqlCacheConfig.class
        ).forEach(c -> {
            storageManager.register(c);
        });
        context.put(StorageManager.class, storageManager);
        MetaClusterCurrent.register(context);

        if (initConfig) {
            ConfigUpdater.writeDefaultConfigToFile();
        }
    }

    private void testZkAddressOrStartDefaultZk(String zkAddress) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        ConnectStringParser connectStringParser = new ConnectStringParser(zkAddress);
        CompositeFuture.any(connectStringParser.getServerAddresses().stream().parallel().map(is -> Future.future(promise -> {
            try {
                Socket socket = new Socket(is.getHostName(), is.getPort());
                socket.close();
                promise.tryComplete();
            } catch (IOException e) {
                promise.tryFail(e);
            }
        })).collect(Collectors.toList())).toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS).recover(throwable -> {
            logger.error("", throwable);
            try {
                EmbeddedZKServer.startDefaultZK();
                return Future.succeededFuture();
            } catch (Throwable throwable1) {
                return Future.failedFuture(throwable1);
            }
        }).toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
    }

    @NotNull
    private String findMycatHome() throws URISyntaxException {
        String configResourceKeyName = "MYCAT_HOME";
        String path = System.getProperty(configResourceKeyName);

        if (path == null) {
            Path bottom = Paths.get(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
            while (!(Files.isDirectory(bottom) && Files.isWritable(bottom))) {
                bottom = bottom.getParent();
            }
            path = bottom.toString();
            System.setProperty(configResourceKeyName,path);
        }
        if (path == null) {
            throw new MycatException("can not find MYCAT_HOME");
        }
        return path;
    }

    @NotNull
    private MycatServer newMycatServer(MycatServerConfig serverConfig) throws URISyntaxException {
        String configResourceKeyName = "server";
        String type = System.getProperty(configResourceKeyName, "vertx");
        if ("native".equalsIgnoreCase(type)) {
            logger.info("start NativeMycatServer");
            return new NativeMycatServer(serverConfig);
        }
        if ("vertx".equalsIgnoreCase(type)) {
            logger.info("start VertxMycatServer");
            return new VertxMycatServer(serverConfig);
        }
        throw new UnsupportedOperationException("unsupport server type:" + type);
    }

    public void startServer() throws Exception {
        ConfigUpdater.loadConfigFromFile();
        mycatServer.start();
    }

    public static void main(String[] args) throws Exception {
        if (args != null) {
            for (String i : args) {
                if (i.startsWith("-D") || i.startsWith("-d")) {
                    i = i.substring(2);
                    if (i.contains("=")) {
                        String[] n = i.substring(2).split("=");
                        System.setProperty(n[0], n[1]);
                    } else {
                        System.setProperty(i, Boolean.TRUE.toString());
                    }
                }
            }
        }
        new MycatCore().startServer();
    }
}
