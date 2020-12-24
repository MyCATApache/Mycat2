package io.mycat;

import io.mycat.config.MycatServerConfig;
import io.mycat.config.ServerConfiguration;
import io.mycat.config.ServerConfigurationImpl;
import io.mycat.exporter.PrometheusExporter;
import io.mycat.gsi.GSIService;
import io.mycat.gsi.mapdb.MapDBGSIService;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.proxy.session.ProxyAuthenticator;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * @author cjw
 **/
public class MycatCore {

    public static final String PROPERTY_MODE_LOCAL = "local";
    public static final String PROPERTY_MODE_CLUSTER = "cluster";
    public static final String PROPERTY_METADATADIR = "metadata";
    private final MycatServer mycatServer;
    private final MetadataStorageManager metadataStorageManager;
    private final Path baseDirectory;

    static {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(classLoader == null){
            classLoader = MycatCore.class.getClassLoader();
        }
        boolean initialize = !Boolean.getBoolean("MYCAT_LAZY_STARTUP");
        try {
            Class.forName("org.apache.calcite.rel.core.Project",initialize,classLoader);
            Class.forName("oshi.util.platform.windows.PerfCounterQuery",initialize,classLoader);
            Class.forName("io.mycat.datasource.jdbc.datasource.JdbcConnectionManager",initialize,classLoader);
            Class.forName("org.apache.calcite.sql.SqlUtil",initialize,classLoader);
            Class.forName("org.apache.calcite.plan.RelOptUtil",initialize,classLoader);
            Class.forName("org.apache.calcite.plan.RelOptUtil",initialize,classLoader);
            Class.forName("org.apache.calcite.mycat.MycatBuiltInMethod",initialize,classLoader);
            Class.forName("com.alibaba.fastsql.sql.SQLUtils",initialize,classLoader);
            Class.forName("com.alibaba.druid.sql.SQLUtils",initialize,classLoader);

        } catch (ClassNotFoundException e) {
            throw new Error("init error. "+e.toString());
        }
    }

    @SneakyThrows
    public MycatCore() {
        String path = null;
        // TimeZone.setDefault(ZoneInfo.getTimeZone("UTC"));
        if (path == null) {
            String configResourceKeyName = "MYCAT_HOME";
            path = System.getProperty(configResourceKeyName);
        }
        boolean enableGSI = true;

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

        this.baseDirectory = Paths.get(path).toAbsolutePath();
        System.out.println("path:" + this.baseDirectory);
        ServerConfiguration serverConfiguration = new ServerConfigurationImpl(MycatCore.class, path);
        MycatServerConfig serverConfig = serverConfiguration.serverConfig();
        String datasourceProvider = serverConfig.getDatasourceProvider();
        this.mycatServer = new MycatServer(serverConfig, new ProxyAuthenticator(), new ProxyDatasourceConfigProvider());
        LoadBalanceManager loadBalanceManager = mycatServer.getLoadBalanceManager();
        MycatWorkerProcessor mycatWorkerProcessor = mycatServer.getMycatWorkerProcessor();

        HashMap<Class, Object> context = new HashMap<>();
        context.put(serverConfig.getServer().getClass(), serverConfig.getServer());
        context.put(serverConfiguration.getClass(), serverConfiguration);
        context.put(serverConfig.getClass(), serverConfig);
        context.put(loadBalanceManager.getClass(), loadBalanceManager);
        context.put(mycatWorkerProcessor.getClass(), mycatWorkerProcessor);
        context.put(mycatServer.getClass(), mycatServer);
        context.put(SqlRecorderRuntime.class, SqlRecorderRuntime.INSTANCE);
        ////////////////////////////////////////////tmp///////////////////////////////////
        if(enableGSI) {
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
                String zkAddress = System.getProperty("zk_address",(String) serverConfig.getProperties().get("zk_address"));
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

    public void start() throws Exception {
        metadataStorageManager.start();
        mycatServer.start();

        new PrometheusExporter().run();
    }

    public static void main(String[] args) throws Exception {
        new MycatCore().start();
    }
}
