package io.mycat.config;

import com.google.common.collect.Lists;
import io.mycat.ConfigOps;
import io.mycat.ConfigReaderWriter;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataStorageManager;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.replica.ReplicaType;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileMetadataStorageManager extends MetadataStorageManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMetadataStorageManager.class);
    private MycatServerConfig serverConfig;
    private final String datasourceProvider;
    private final Path baseDirectory;


    @SneakyThrows
    public FileMetadataStorageManager(MycatServerConfig serverConfig, String datasourceProvider, Path baseDirectory) {
        this.serverConfig = serverConfig;
        this.datasourceProvider = datasourceProvider;
        this.baseDirectory = baseDirectory;
    }

    @NotNull
    private ConfigReaderWriter getConfigReaderWriter(Path metafile) {
        String fileName = metafile.toString();
        String suffix = getSuffix(fileName);
        return ConfigReaderWriter.getReaderWriterBySuffix(suffix);
    }

    @NotNull
    private String getSuffix(String fileName) {
        return fileName.substring(fileName.lastIndexOf(".") + 1);
    }

    Path resolveFileName(String name) {
        Path f = baseDirectory.resolve(name + ".yml");
        Path s = baseDirectory.resolve(name + ".yaml");
        Path t = baseDirectory.resolve(name + ".json");
        return Files.exists(f) ? f : (Files.exists(s) ? s : Files.exists(t) ? t : baseDirectory.resolve(name + ".json"));
    }

    boolean isSuffix(Path path, String suffix) {
        String s1 = path.toString();
        boolean b = s1.endsWith(suffix + ".yml");
        boolean b2 = s1.endsWith(suffix + ".yaml");
        boolean b3 = s1.endsWith(suffix + ".json");
        return b || b2 || b3;
    }

    @SneakyThrows
    String readString(Path path) {
        return new String(Files.readAllBytes(path));
    }

    @NotNull
    @SneakyThrows
    private MycatRouterConfig getRouterConfig(Path baseDirectory) {
//        Path mycatPath = resolveFileName("mycat");
//        String suffix = getSuffix(mycatPath.toString());
        Path schemasPath = baseDirectory.resolve("schemas");
        Path clustersPath = baseDirectory.resolve("clusters");
        Path datasources = baseDirectory.resolve("datasources");
        Path users = baseDirectory.resolve("users");
        Path sequences = baseDirectory.resolve("sequences");
        Path sqlcaches = baseDirectory.resolve("sqlcaches");
        if (Files.notExists(schemasPath)) Files.createDirectory(schemasPath);
        if (Files.notExists(clustersPath)) Files.createDirectory(clustersPath);
        if (Files.notExists(datasources)) Files.createDirectory(datasources);
        if (Files.notExists(users)) Files.createDirectory(users);
        if (Files.notExists(sequences)) Files.createDirectory(sequences);
        if (Files.notExists(sqlcaches)) Files.createDirectory(sqlcaches);
//        if (Files.notExists(mycatPath)) {
//            writeFile(getConfigReaderWriter(mycatPath).transformation(new MycatRouterConfig()),mycatPath);
//        }

        Stream<Path> schemaPaths = Files.list(baseDirectory.resolve("schemas")).filter(i -> isSuffix(i, "schema"));
        Stream<Path> clusterPaths = Files.list(baseDirectory.resolve("clusters")).filter(i -> isSuffix(i, "cluster"));
        Stream<Path> datasourcePaths = Files.list(baseDirectory.resolve("datasources")).filter(i -> isSuffix(i, "datasource"));
        Stream<Path> userPaths = Files.list(baseDirectory.resolve("users")).filter(i -> isSuffix(i, "user"));
        Stream<Path> sequencePaths = Files.list(baseDirectory.resolve("sequences")).filter(i -> isSuffix(i, "sequence"));
        Stream<Path> sqlcachePaths = Files.list(baseDirectory.resolve("sqlcaches")).filter(i -> isSuffix(i, "sqlcache"));

        MycatRouterConfig routerConfig = new MycatRouterConfig();

        List<LogicSchemaConfig> logicSchemaConfigs = schemaPaths.map(i -> {
            ConfigReaderWriter configReaderWriter = getConfigReaderWriter(i);
            String s = readString(i);
            LogicSchemaConfig schemaConfig = configReaderWriter.transformation(s, LogicSchemaConfig.class);
            return schemaConfig;
        }).distinct().collect(Collectors.toList());


        List<ClusterConfig> clusterConfigs = clusterPaths.map(i -> {
            ConfigReaderWriter configReaderWriter = getConfigReaderWriter(i);
            return configReaderWriter.transformation(readString(i), ClusterConfig.class);
        }).distinct().collect(Collectors.toList());

        List<DatasourceConfig> datasourceConfigs = datasourcePaths.map(i -> {
            ConfigReaderWriter configReaderWriter = getConfigReaderWriter(i);
            return configReaderWriter.transformation(readString(i), DatasourceConfig.class);
        }).distinct().collect(Collectors.toList());

        List<UserConfig> userConfigs = userPaths.map(i -> {
            ConfigReaderWriter configReaderWriter = getConfigReaderWriter(i);
            return configReaderWriter.transformation(readString(i), UserConfig.class);
        }).distinct().collect(Collectors.toList());

        List<SequenceConfig> sequenceConfigs = sequencePaths.map(i -> {
            ConfigReaderWriter configReaderWriter = getConfigReaderWriter(i);
            return configReaderWriter.transformation(readString(i), SequenceConfig.class);
        }).distinct().collect(Collectors.toList());

        List<SqlCacheConfig> cacheConfigs = sqlcachePaths.map(i -> {
            ConfigReaderWriter configReaderWriter = getConfigReaderWriter(i);
            return configReaderWriter.transformation(readString(i), SqlCacheConfig.class);
        }).distinct().collect(Collectors.toList());

        routerConfig.getSchemas().addAll(logicSchemaConfigs);
        routerConfig.getClusters().addAll(clusterConfigs);
        routerConfig.getDatasources().addAll(datasourceConfigs);
        routerConfig.getUsers().addAll(userConfigs);
        routerConfig.getSequences().addAll(sequenceConfigs);
        routerConfig.getSqlCacheConfigs().addAll(cacheConfigs);

        defaultConfig(routerConfig);

        routerConfig.setSchemas(routerConfig.getSchemas().stream().distinct().collect(Collectors.toList()));
        routerConfig.setClusters(routerConfig.getClusters().stream().distinct().collect(Collectors.toList()));
        routerConfig.setDatasources(routerConfig.getDatasources().stream().distinct().collect(Collectors.toList()));
        routerConfig.setUsers(routerConfig.getUsers().stream().distinct().collect(Collectors.toList()));
        routerConfig.setSequences(routerConfig.getSequences().stream().distinct().collect(Collectors.toList()));

        return routerConfig;
    }

    public static void defaultConfig(MycatRouterConfig routerConfig) {
        if (routerConfig.getUsers().isEmpty()) {
            UserConfig userConfig = new UserConfig();
            userConfig.setPassword("123456");
            userConfig.setUsername("root");
            routerConfig.getUsers().add(userConfig);
        }

        if (routerConfig.getDatasources().isEmpty()) {
            DatasourceConfig datasourceConfig = new DatasourceConfig();
            datasourceConfig.setDbType("mysql");
            datasourceConfig.setUser("root");
            datasourceConfig.setPassword("123456");
            datasourceConfig.setName("prototypeDs");
            datasourceConfig.setUrl("jdbc:mysql://127.0.0.1:3306/mysql");
            routerConfig.getDatasources().add(datasourceConfig);
        }
        if (routerConfig.getClusters().isEmpty()) {
            ClusterConfig clusterConfig = new ClusterConfig();
            clusterConfig.setName("prototype");
            clusterConfig.setMasters(Lists.newArrayList("prototypeDs"));
            clusterConfig.setMaxCon(200);
            clusterConfig.setClusterType(ReplicaType.MASTER_SLAVE.name());
            clusterConfig.setSwitchType(ReplicaSwitchType.SWITCH.name());
            routerConfig.getClusters().add(clusterConfig);
        }
    }

    @Override
    @SneakyThrows
   public void start() {
        try (ConfigOps configOps = startOps()) {
            configOps.commit(new MycatRouterConfigOps((io.mycat.config.MycatRouterConfig) loadFromLocalFile(), configOps));
        }catch (Exception e){
           throw  MycatErrorCode.createMycatException(MycatErrorCode.ERR_INIT_CONFIG,"start FileMetadataStorageManager fail",e);
        }
    }

    @Override
    @SneakyThrows
    public void reportReplica(Map<String, Set<String>> setMap) {
        Path statePath = baseDirectory.resolve("state.json");
        final State state = new State();
        state.replica.putAll(setMap);
        writeFile(
                ConfigReaderWriter.getReaderWriterBySuffix("json")
                        .transformation(state), statePath);

    }

    @EqualsAndHashCode
    @Data
    public static class State {
        final Map<String, Set<String>> replica = new HashMap<>();
    }

    @Override
    @SneakyThrows
    public ConfigOps startOps() {
        Path lockFile = baseDirectory.resolve("mycat.lock");
        if (Files.notExists(lockFile)) Files.createFile(lockFile);
        FileChannel lockFileChannel = FileChannel.open(lockFile, StandardOpenOption.WRITE);
        FileLock lock = Objects.requireNonNull(lockFileChannel.lock());
        return new ConfigOps() {

            @Override
            @SneakyThrows
            public Object currentConfig() {
                return MetaClusterCurrent.wrapper(MycatRouterConfig.class);
            }

            @Override
            public void commit(Object ops) throws Exception {
                commitAndSyncDisk((MycatRouterConfigOps) ops);
            }

            @Override
            public void close() {
                try {
                    if (lockFileChannel.isOpen()) {
                        lock.release();
                        lockFileChannel.close();
                    }
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
        };
    }

    public State commitAndSyncDisk(MycatRouterConfigOps ops) throws IOException {
        String suffix = "json";
        MycatRouterConfigOps routerConfig = ops;
        ConfigPrepareExecuter prepare = new ConfigPrepareExecuter(routerConfig, FileMetadataStorageManager.this, datasourceProvider);
        prepare.prepareRuntimeObject();
        prepare.prepareStoreDDL();

        Path schemasPath = baseDirectory.resolve("schemas");
        Path clustersPath = baseDirectory.resolve("clusters");
        Path datasources = baseDirectory.resolve("datasources");
        Path users = baseDirectory.resolve("users");
        Path sequences = baseDirectory.resolve("sequences");
        Path sqlcaches = baseDirectory.resolve("sqlcaches");

        if (routerConfig.isUpdateSchemas()) {
            cleanDirectory(schemasPath);
        }
        if (routerConfig.isUpdateClusters()) {
            cleanDirectory(clustersPath);
        }
        if (routerConfig.isUpdateDatasources()) {
            cleanDirectory(datasources);
        }
        if (routerConfig.isUpdateUsers()) {
            cleanDirectory(users);
        }
        if (routerConfig.isUpdateSequences()) {
            cleanDirectory(sequences);
        }
        if (routerConfig.isUpdateSqlCaches()) {
            cleanDirectory(sqlcaches);
        }

        for (LogicSchemaConfig schemaConfig : Optional.ofNullable(routerConfig.getSchemas()).orElse(Collections.emptyList())) {
            String fileName = schemaConfig.getSchemaName() + ".schema." + suffix;
            ConfigReaderWriter readerWriterBySuffix = ConfigReaderWriter.getReaderWriterBySuffix(suffix);
            String t = readerWriterBySuffix.transformation(schemaConfig);
            Path filePath = schemasPath.resolve(fileName);
            writeFile(t, filePath);
        }

        for (DatasourceConfig datasourceConfig : Optional.ofNullable(routerConfig.getDatasources()).orElse(Collections.emptyList())) {
            String fileName = datasourceConfig.getName() + ".datasource." + suffix;
            ConfigReaderWriter readerWriterBySuffix = ConfigReaderWriter.getReaderWriterBySuffix(suffix);
            String t = readerWriterBySuffix.transformation(datasourceConfig);
            Path filePath = datasources.resolve(fileName);
            writeFile(t, filePath);
        }
        for (UserConfig userConfig : Optional.ofNullable(routerConfig.getUsers()).orElse(Collections.emptyList())) {
            String fileName = userConfig.getUsername() + ".user." + suffix;
            ConfigReaderWriter readerWriterBySuffix = ConfigReaderWriter.getReaderWriterBySuffix(suffix);
            String t = readerWriterBySuffix.transformation(userConfig);
            Path filePath = users.resolve(fileName);
            writeFile(t, filePath);
        }
        for (SequenceConfig sequenceConfig : Optional.ofNullable(routerConfig.getSequences()).orElse(Collections.emptyList())) {
            String fileName = sequenceConfig.getName() + ".sequence." + suffix;
            ConfigReaderWriter readerWriterBySuffix = ConfigReaderWriter.getReaderWriterBySuffix(suffix);
            String t = readerWriterBySuffix.transformation(sequenceConfig);
            Path filePath = sequences.resolve(fileName);
            writeFile(t, filePath);
        }
        for (ClusterConfig i : Optional.ofNullable(routerConfig.getClusters()).orElse(Collections.emptyList())) {
            String fileName = i.getName() + ".cluster." + suffix;
            ConfigReaderWriter readerWriterBySuffix = ConfigReaderWriter.getReaderWriterBySuffix(suffix);
            String t = readerWriterBySuffix.transformation(i);
            Path filePath = clustersPath.resolve(fileName);
            writeFile(t, filePath);
        }
        for (SqlCacheConfig i : Optional.ofNullable(routerConfig.getSqlCaches()).orElse(Collections.emptyList())) {
            String fileName = i.getName() + ".sqlcache." + suffix;
            ConfigReaderWriter readerWriterBySuffix = ConfigReaderWriter.getReaderWriterBySuffix(suffix);
            String t = readerWriterBySuffix.transformation(i);
            Path filePath = sqlcaches.resolve(fileName);
            writeFile(t, filePath);
        }
        State state =  new State();
        ReplicaSelectorRuntime replicaSelector = Optional.ofNullable(prepare.getReplicaSelector()).orElseGet(()->MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class));
        state.replica.putAll(replicaSelector.getState());
        prepare.commit();
        Path statePath = baseDirectory.resolve("state.json");

        Files.deleteIfExists(statePath);
        if (Files.notExists(statePath)) Files.createFile(statePath);
        writeFile(
                ConfigReaderWriter.getReaderWriterBySuffix("json")
                        .transformation(state), statePath);

        return state;
    }


    @NotNull
    private MycatRouterConfig loadFromLocalFile() {
        return getRouterConfig(baseDirectory);
    }

    @SneakyThrows
    public void cleanDirectory(Path path) {
        if (Files.exists(path)){
            org.apache.commons.io.FileUtils.cleanDirectory(path.toFile());
        }
    }

    private void writeFile(String t, Path filePath) throws IOException {
        if (Files.exists(filePath)) {
            if (readString(filePath).equals(t)) {
                return;
            }
        }
        FileUtils.write(filePath.toFile(), t);
    }

}