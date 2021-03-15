package io.mycat;

import io.mycat.commands.MycatdbCommand;
import io.mycat.config.*;
import io.mycat.connectionschedule.Scheduler;
import io.mycat.exporter.PrometheusExporter;
import io.mycat.gsi.GSIService;
import io.mycat.gsi.mapdb.MapDBGSIService;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import io.mycat.vertx.VertxMycatServer;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import lombok.SneakyThrows;
import org.apache.calcite.util.RxBuiltInMethod;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author cjw
 **/
public class MycatCore {
    final static Logger logger = LoggerFactory.getLogger(MycatdbCommand.class);
    public static final String PROPERTY_MODE_LOCAL = "local";
    public static final String PROPERTY_MODE_CLUSTER = "cluster";
    public static final String PROPERTY_METADATADIR = "metadata";
    private final MycatServer mycatServer;
    private final MetadataStorageManager metadataStorageManager;
    private final Path baseDirectory;
    private final MycatWorkerProcessor mycatWorkerProcessor;

    static {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = MycatCore.class.getClassLoader();
        }
        boolean initialize = !Boolean.getBoolean("MYCAT_LAZY_STARTUP");
        try {
            Class.forName("org.apache.calcite.rel.core.Project", initialize, classLoader);
            Class.forName("oshi.util.platform.windows.PerfCounterQuery", initialize, classLoader);
            Class.forName("io.mycat.datasource.jdbc.datasource.JdbcConnectionManager", initialize, classLoader);
            Class.forName("org.apache.calcite.sql.SqlUtil", initialize, classLoader);
            Class.forName("org.apache.calcite.plan.RelOptUtil", initialize, classLoader);
            Class.forName("org.apache.calcite.plan.RelOptUtil", initialize, classLoader);
            Class.forName("org.apache.calcite.mycat.MycatBuiltInMethod", initialize, classLoader);
            Class.forName("com.alibaba.druid.sql.SQLUtils", initialize, classLoader);
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
        String datasourceProvider = Optional.ofNullable(serverConfig.getDatasourceProvider()).orElse(io.mycat.datasource.jdbc.DruidDatasourceProvider.class.getCanonicalName());
        ThreadPoolExecutorConfig workerPool = serverConfig.getServer().getWorkerPool();
        this.mycatWorkerProcessor = new MycatWorkerProcessor(workerPool, serverConfig.getServer().getTimeWorkerPool());


        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setWorkerPoolSize(workerPool.getMaxPoolSize());
        vertxOptions.setMaxWorkerExecuteTime(workerPool.getTaskTimeout());
        vertxOptions.setMaxWorkerExecuteTimeUnit(TimeUnit.valueOf(workerPool.getTimeUnit()));


        this.mycatServer = newMycatServer(serverConfig);

        HashMap<Class, Object> context = new HashMap<>();
        Scheduler scheduler = new Scheduler(TimeUnit.valueOf(workerPool.getTimeUnit()).toMillis(workerPool.getTaskTimeout()));
        Thread thread = new Thread(scheduler, "mycat connection scheduler");
        thread.start();
        context.put(Scheduler.class,scheduler);
        context.put(serverConfig.getServer().getClass(), serverConfig.getServer());
        context.put(serverConfiguration.getClass(), serverConfiguration);
        context.put(serverConfig.getClass(), serverConfig);
        context.put(LoadBalanceManager.class, new LoadBalanceManager(serverConfig.getLoadBalance()));
        context.put(Vertx.class, Vertx.vertx(vertxOptions));
        context.put(mycatWorkerProcessor.getClass(), mycatWorkerProcessor);
        context.put(this.mycatServer.getClass(), mycatServer);
        context.put(MycatServer.class, mycatServer);
        context.put(SqlRecorderRuntime.class, SqlRecorderRuntime.INSTANCE);
        ////////////////////////////////////////////tmp///////////////////////////////////
        if (enableGSI) {
            File gsiMapDBFile = baseDirectory.resolve("gsi.db").toFile();
            context.put(GSIService.class, new MapDBGSIService(gsiMapDBFile, null));
        }
        MetaClusterCurrent.register(context);

        String mode = serverConfig.getMode();
        switch (mode) {
            case PROPERTY_MODE_LOCAL: {
                metadataStorageManager = new FileMetadataStorageManager(serverConfig, datasourceProvider, this.baseDirectory);
                break;
            }
            case PROPERTY_MODE_CLUSTER:
                String zkAddress = System.getProperty("zk_address", (String) serverConfig.getProperties().get("zk_address"));
                if (zkAddress != null) {
                    metadataStorageManager =
                            new CoordinatorMetadataStorageManager(
                                    new FileMetadataStorageManager(serverConfig,
                                            datasourceProvider,
                                            this.baseDirectory),
                                    zkAddress);
                    break;
                }
            default: {
                throw new UnsupportedOperationException();
            }
        }

        context.put(metadataStorageManager.getClass(), metadataStorageManager);
        MetaClusterCurrent.register(context);
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

    public void start() throws Exception {
        metadataStorageManager.start();
        mycatServer.start();

        new PrometheusExporter().run();
    }

    public static void main(String[] args) throws Exception {
        if (args != null) {
            Arrays.stream(args).filter(i -> i.startsWith("-D")||i.startsWith("-d"))
                    .map(i -> i.substring(2).split("=")).forEach(n -> System.setProperty(n[0], n[1]));
        }
        new MycatCore().start();
    }
}
