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
import io.mycat.config.*;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import io.mycat.ui.*;
import io.mycat.vertx.VertxMycatServer;
import io.mycat.vertxmycat.MycatVertxMetricsFactory;
import io.vertx.core.*;
import lombok.SneakyThrows;
import org.apache.calcite.util.RxBuiltInMethod;
import org.apache.curator.framework.CuratorFramework;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
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
    private final MetadataStorageManager metadataStorageManager;
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
        context.put(IOExecutor.class, IOExecutor.fromVertx(vertx));
        context.put(serverConfig.getServer().getClass(), serverConfig.getServer());
        context.put(serverConfiguration.getClass(), serverConfiguration);
        context.put(serverConfig.getClass(), serverConfig);
        context.put(LoadBalanceManager.class, new LoadBalanceManager(serverConfig.getLoadBalance()));
        context.put(Vertx.class, vertx);
        context.put(this.mycatServer.getClass(), mycatServer);
        context.put(MycatServer.class, mycatServer);
        context.put(SqlRecorderRuntime.class, SqlRecorderRuntime.INSTANCE);

        ////////////////////////////////////////////tmp///////////////////////////////////
        MetaClusterCurrent.register(context);

        String mode = Optional.ofNullable(System.getProperty("mode"))
                .orElse(serverConfig.getMode());
        switch (mode) {
            case PROPERTY_MODE_LOCAL: {
                context.put(LockService.class, new LocalLockServiceImpl());
                metadataStorageManager = new FileMetadataStorageManager(serverConfig, datasourceProvider, this.baseDirectory);
                break;
            }
            case PROPERTY_MODE_CLUSTER:
                String zkAddress = System.getProperty("zk_address", (String) serverConfig.getProperties().get("zk_address"));
                if (zkAddress != null) {
                    testZkAddressOrStartDefaultZk(zkAddress);
                } else {
                    zkAddress = "localhost:2181";
                    EmbeddedZKServer.startDefaultZK();
                }
                ZKBuilder zkBuilder = new ZKBuilder(zkAddress);
                context.put(LockService.class, new ZKLockServiceImpl());
                CuratorFramework curatorFramework = zkBuilder.build();
                context.put(CuratorFramework.class, curatorFramework);
                metadataStorageManager =
                        new CoordinatorMetadataStorageManager(
                                new FileMetadataStorageManager(serverConfig,
                                        datasourceProvider,
                                        this.baseDirectory),
                                curatorFramework);
                break;
            default: {
                throw new UnsupportedOperationException();
            }
        }

        context.put(metadataStorageManager.getClass(), metadataStorageManager);
        context.put(MetadataStorageManager.class, metadataStorageManager);
        MetaClusterCurrent.register(context);
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
        metadataStorageManager.start();
       if (Boolean.getBoolean("ui")){
           UIMain.infoProviderFactory = new InfoProviderFactory() {
               @Override
               public InfoProvider create(InfoProviderType type, Map<String,String> args) {
                   switch (type) {
                       case LOCAL:
                           return new LocalInfoProvider();
                       case TCP:
                           return  new TcpInfoProvider(args.get("url"),args.get("user"),args.get("password"));
                       default:
                           throw new IllegalStateException("Unexpected value: " + type);
                   }

               };
           };

           new Thread(()->{
               System.out.println("UI Thread:"+Thread.currentThread());
               UIMain.main(null);
           }).start();
       }
       mycatServer.start();
    }

    public static void main(String[] args) throws Exception {
        if (args != null) {
            for (String i : args) {
                if (i.startsWith("-D") || i.startsWith("-d")) {
                    i = i.substring(2);
                    if (i.contains("=")){
                        String[] n = i.substring(2).split("=");
                        System.setProperty(n[0], n[1]);
                    }else {
                        System.setProperty(i,Boolean.TRUE.toString());
                    }
                }
            }
        }
        new MycatCore().startServer();
    }
}
