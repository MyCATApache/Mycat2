package io.mycat;

import io.mycat.config.MycatServerConfig;
import io.mycat.config.ServerConfiguration;
import io.mycat.config.ServerConfigurationImpl;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.proxy.session.ProxyAuthenticator;
import lombok.SneakyThrows;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

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

    public MycatCore() {
        this(null);
    }

    @SneakyThrows
    public MycatCore(String path) {
        if (path == null) {
            String configResourceKeyName = "MYCAT_HOME";
            path = System.getProperty(configResourceKeyName);
        }
        if (path == null) {
            path = Paths.get(Thread.currentThread().getClass().getResource("/").toURI()).toAbsolutePath().toString();
        }
        this.baseDirectory = Paths.get(path).toAbsolutePath();
        System.out.println("path:" + this.baseDirectory);
        ServerConfiguration serverConfiguration = new ServerConfigurationImpl(MycatCore.class, path);
        MycatServerConfig serverConfig = serverConfiguration.serverConfig();
        String datasourceProvider = serverConfig.getDatasourceProvider();
        this.mycatServer = new MycatServer(serverConfig, new ProxyAuthenticator(), new ProxyDatasourceConfigProvider());
        LoadBalanceManager loadBalanceManager = mycatServer.getLoadBalanceManager();
        MycatWorkerProcessor mycatWorkerProcessor = mycatServer.getMycatWorkerProcessor();

        MetaClusterCurrent.register(serverConfiguration);
        MetaClusterCurrent.register(serverConfig);
        MetaClusterCurrent.register(loadBalanceManager);
        MetaClusterCurrent.register(mycatWorkerProcessor);
        MetaClusterCurrent.register(mycatServer);


        String mode = Optional.ofNullable(serverConfig.getMode()).orElse(PROPERTY_MODE_LOCAL).toLowerCase();
        switch (mode) {
            case PROPERTY_MODE_LOCAL: {
                metadataStorageManager = new FileMetadataStorageManager(datasourceProvider, this.baseDirectory);
                break;
            }
            case PROPERTY_MODE_CLUSTER:
            default: {
                throw new UnsupportedOperationException();
            }
        }
        MetaClusterCurrent.register(metadataStorageManager);
    }

    private void start() {
        metadataStorageManager.start();
        mycatServer.start();
    }

    public static void main(String[] args) {
        new MycatCore().start();
    }
}
