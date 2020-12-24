package io.mycat;

import io.mycat.config.*;
import io.mycat.exporter.PrometheusExporter;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.proxy.session.ProxyAuthenticator;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import lombok.SneakyThrows;
import org.apache.calcite.mycat.MycatBuiltInMethod;

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

    @SneakyThrows
    public MycatCore() {
        String path = null;
        MycatBuiltInMethod booleanToBigint = MycatBuiltInMethod.BOOLEAN_TO_BIGINT;
        // TimeZone.setDefault(ZoneInfo.getTimeZone("UTC"));
        if (path == null) {
            String configResourceKeyName = "MYCAT_HOME";
            path = System.getProperty(configResourceKeyName);
        }
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
//        File gsiMapDBFile = baseDirectory.resolve("gsi.db").toFile();
//        context.put(GSIService.class,new MapDBGSIService(gsiMapDBFile,null));
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
