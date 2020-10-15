package io.mycat;

import io.mycat.client.InterceptorRuntime;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.metadata.MetadataManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.proxy.session.Authenticator;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.YamlUtil;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FileMetadataStorageManager extends MetadataStorageManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMetadataStorageManager.class);
    private final String datasourceProvider;
    private final Path baseDirectory;
    private final Path metafile;

    @SneakyThrows
    public FileMetadataStorageManager(String datasourceProvider, Path baseDirectory) {
        this.datasourceProvider = datasourceProvider;
        this.baseDirectory = baseDirectory;
        this.metafile = baseDirectory.resolve("metadata.yml");

    }

    private void listen(Path file) throws IOException {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        file.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
        ScheduledExecutorService timer = ScheduleUtil.getTimer();
        timer.scheduleAtFixedRate(() -> {
            WatchKey key;
            try {
                while ((key = watchService.take()) != null) {
                    key.reset();
                    init();
                }
            } catch (Throwable t) {
                LOGGER.error("", t);
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    @SneakyThrows
    private void init() {
        Path interceptorPath = baseDirectory.resolve("interceptor.yml");
        if (Files.exists(interceptorPath)) {
            InterceptorRootConfig mycatRouterConfig = YamlUtil.load(InterceptorRootConfig.class, new FileReader(interceptorPath.toFile()));
            InterceptorRuntime interceptorRuntime = new InterceptorRuntime(mycatRouterConfig.getPatternRootConfigs());
            MetaClusterCurrent.register(interceptorRuntime);
        }


        System.out.println(YamlUtil.dump(new MycatRouterConfig()));

        LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        MycatRouterConfig mycatRouterConfig = YamlUtil.load(MycatRouterConfig.class, new FileReader(baseDirectory.resolve("metadata.yml").toFile()));
        SequenceGenerator sequenceGenerator = new SequenceGenerator(mycatRouterConfig.getSequences());
        Map<String, DatasourceConfig> datasourceConfigMap = mycatRouterConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        ClusterRootConfig clusterRootConfig = mycatRouterConfig.getCluster();
        Map<String, ClusterConfig> clusters = clusterRootConfig.getClusters().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        ReplicaSelectorRuntime replicaSelector = new ReplicaSelectorRuntime(clusterRootConfig, datasourceConfigMap, loadBalanceManager, this);
        JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(datasourceProvider, datasourceConfigMap, clusters, mycatWorkerProcessor, replicaSelector);
        MetadataManager metadataManager = new MetadataManager(mycatRouterConfig.getShardingQueryRootConfig(), loadBalanceManager, sequenceGenerator, replicaSelector, jdbcConnectionManager);
        MetaClusterCurrent.register(mycatRouterConfig);
        MetaClusterCurrent.register(Authenticator.class, new AuthenticatorImpl(mycatRouterConfig.getUsers().stream().collect(Collectors.toMap(k -> k.getUsername(), v -> v))));
        MetaClusterCurrent.register(DatasourceConfigProvider.class, new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return datasourceConfigMap;
            }
        });
        MetaClusterCurrent.register(clusterRootConfig);
        MetaClusterCurrent.register(replicaSelector);
        MetaClusterCurrent.register(jdbcConnectionManager);
        MetaClusterCurrent.register(metadataManager);
    }

    @Override
    @SneakyThrows
    void start() {
        init();
        listen(this.baseDirectory);
    }

    @Override
    public void reportReplica(String name, Set<String> dsNames) {

    }
}