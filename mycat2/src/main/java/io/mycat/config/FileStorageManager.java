package io.mycat.config;

import io.mycat.ConfigReaderWriter;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileStorageManager implements BaseMetadataStorageManager {
    private final Path baseDirectory;
    private Path schemasPath;
    private Path clustersPath;
    private Path datasources;
    private Path users;
    private Path sequences;
    private Path sqlcaches;
    private static String suffix = "json";

    public FileStorageManager(Path baseDirectory) {
        this.baseDirectory = baseDirectory;


        this.schemasPath = baseDirectory.resolve("schemas");
        this.clustersPath = baseDirectory.resolve("clusters");
        this.datasources = baseDirectory.resolve("datasources");
        this.users = baseDirectory.resolve("users");
        this.sequences = baseDirectory.resolve("sequences");
        this.sqlcaches = baseDirectory.resolve("sqlcaches");
    }

    @SneakyThrows
    private void writeFile(String t, Path filePath) {
        FileUtils.write(filePath.toFile(), t);
    }


    private void syncSchema(List<LogicSchemaConfig> schemaConfigs) {
        for (LogicSchemaConfig schemaConfig : schemaConfigs) {
            String schemaName = schemaConfig.getSchemaName();
            String fileName = getSchemaFilePath(schemaName);
            ConfigReaderWriter readerWriterBySuffix = ConfigReaderWriter.getReaderWriterBySuffix(suffix);
            String t = readerWriterBySuffix.transformation(schemaConfig);
            Path filePath = schemasPath.resolve(fileName);
            writeFile(t, filePath);
        }
    }

    @NotNull
    private String getSchemaFilePath(String schemaName) {
        return schemaName + ".schema." + suffix;
    }

    @NotNull
    private String getSequenceFilePath(String schemaName) {
        return schemaName + ".sequence." + suffix;
    }

    @NotNull
    private String getReplicaFilePath(String schemaName) {
        return schemaName + ".cluster." + suffix;
    }

    @NotNull
    private String getCacheFilePath(String name) {
        return name + ".sqlcache." + suffix;
    }

    @NotNull
    private String getUserFilePath(String user) {
        return user + ".user." + suffix;
    }

    @NotNull
    private String getDatasourceFilePath(String user) {
        return user + ".datasource." + suffix;
    }

    @Override
    public void putSchema(LogicSchemaConfig schemaConfig) {
        syncSchema(Collections.singletonList(schemaConfig));
    }

    @Override
    @SneakyThrows
    public void dropSchema(String schemaName) {
        Files.deleteIfExists(schemasPath.resolve(getSchemaFilePath(schemaName)));
    }

    @Override
    @SneakyThrows
    public void putTable(CreateTableConfig createTableConfig) {
        String schemaFilePath = getSchemaFilePath(createTableConfig.getSchemaName());
        LogicSchemaConfig logicSchemaConfig = Json.decodeValue(new String(Files.readAllBytes((schemasPath.resolve(schemaFilePath)))), LogicSchemaConfig.class);

        String schemaName = createTableConfig.getSchemaName();
        String tableName = createTableConfig.getTableName();

        GlobalTableConfig globalTable = createTableConfig.getGlobalTable();
        NormalTableConfig normalTable = createTableConfig.getNormalTable();
        ShardingTableConfig shardingTable = createTableConfig.getShardingTable();

        if (globalTable != null) {
            Map<String, GlobalTableConfig> globalTables = new HashMap<>(logicSchemaConfig.getGlobalTables());
            globalTables.put(tableName, globalTable);
            logicSchemaConfig.setGlobalTables(globalTables);
        }
        if (normalTable != null) {
            HashMap<String, NormalTableConfig> normals = new HashMap<>(logicSchemaConfig.getNormalTables());
            normals.put(tableName, normalTable);
            logicSchemaConfig.setNormalTables(normals);
        }
        if (shardingTable != null) {
            HashMap<String, ShardingTableConfig> shardingTableConfigHashMap = new HashMap<>(logicSchemaConfig.getShardingTables());
            shardingTableConfigHashMap.put(tableName, shardingTable);
            logicSchemaConfig.setShardingTables(shardingTableConfigHashMap);
        }
        putSchema(logicSchemaConfig);
    }


    @Override
    @SneakyThrows
    public void removeTable(String schemaNameArg, String tableNameArg) {

        String schemaFilePath = getSchemaFilePath(schemaNameArg);
        LogicSchemaConfig logicSchemaConfig = Json.decodeValue(new String(Files.readAllBytes(schemasPath.resolve(schemaFilePath))), LogicSchemaConfig.class);

        HashMap<String, GlobalTableConfig> globalTableConfigHashMap = new HashMap<>(logicSchemaConfig.getGlobalTables());
        globalTableConfigHashMap.remove(tableNameArg);

        Map<String, NormalTableConfig> normalTables = new HashMap<>(logicSchemaConfig.getNormalTables());
        normalTables.remove(tableNameArg);

        HashMap<String, ShardingTableConfig> shardingTableConfigHashMap = new HashMap<>(logicSchemaConfig.getShardingTables());
        shardingTableConfigHashMap.remove(tableNameArg);

        logicSchemaConfig.setGlobalTables(globalTableConfigHashMap);
        logicSchemaConfig.setNormalTables(normalTables);
        logicSchemaConfig.setShardingTables(shardingTableConfigHashMap);

        dropSchema(schemaNameArg);
        putSchema(logicSchemaConfig);
    }

    @Override
    @SneakyThrows
    public void putUser(UserConfig userConfig) {
        String userFilePath = getUserFilePath(userConfig.getUsername());
      writeFile(Json.encodePrettily(userConfig),users.resolve(userFilePath));
    }

    @Override
    @SneakyThrows
    public void deleteUser(String username) {
        String userFilePath = getUserFilePath(username);
        Files.deleteIfExists(users.resolve(userFilePath));
    }

    @Override
    @SneakyThrows
    public void putSequence(SequenceConfig sequenceConfig) {
        String sequenceFilePath = getSequenceFilePath(sequenceConfig.getName());
        writeFile( Json.encodePrettily(sequenceConfig),sequences.resolve(sequenceFilePath));
    }

    @Override
    @SneakyThrows
    public void removeSequenceByName(String name) {
        String userFilePath = getSequenceFilePath(name);
        Files.deleteIfExists(sequences.resolve(userFilePath));
    }

    @Override
    @SneakyThrows
    public void putDatasource(DatasourceConfig datasourceConfig) {
        String datasourceFilePath = getDatasourceFilePath(datasourceConfig.getName());
        writeFile(Json.encodePrettily(datasourceConfig),datasources.resolve(datasourceFilePath));
    }

    @Override
    @SneakyThrows
    public void removeDatasource(String datasourceName) {
        String datasourceFilePath = getDatasourceFilePath(datasourceName);
        Files.deleteIfExists(datasources.resolve(datasourceFilePath));
    }

    @Override
    @SneakyThrows
    public void putReplica(ClusterConfig clusterConfig) {
        String replicaFilePath = getReplicaFilePath(clusterConfig.getName());
        writeFile(Json.encodePrettily(clusterConfig), clustersPath.resolve(replicaFilePath));
    }

    @Override
    @SneakyThrows
    public void removeReplica(String replicaName) {
        Files.deleteIfExists(clustersPath.resolve(replicaName));
    }


    @Override
    public void sync(MycatRouterConfig mycatRouterConfig) {
        List<LogicSchemaConfig> schemas = mycatRouterConfig.getSchemas();
        List<ClusterConfig> clusters = mycatRouterConfig.getClusters();
        List<DatasourceConfig> datasources = mycatRouterConfig.getDatasources();
        List<UserConfig> users = mycatRouterConfig.getUsers();// users/xxx.user.yml
        List<SequenceConfig> sequences = mycatRouterConfig.getSequences();// sequences/xxx.sequence.yml
        List<SqlCacheConfig> sqlCacheConfigs = mycatRouterConfig.getSqlCacheConfigs();

        cleanDirectory(this.schemasPath);
        cleanDirectory(this.clustersPath);
        cleanDirectory(this.datasources);
        cleanDirectory(this.users);
        cleanDirectory(this.sequences);

        syncSchema(schemas);
        clusters.forEach(c -> putReplica(c));
        datasources.forEach(d -> putDatasource(d));
        users.forEach(u -> putUser(u));
        sequences.forEach(s -> putSequence(s));
        sqlCacheConfigs.forEach(s -> putSqlCache(s));
    }

    @Override
    @SneakyThrows
    public void putSqlCache(SqlCacheConfig sqlCacheConfig) {
        String cacheFilePath = getCacheFilePath(sqlCacheConfig.getName());
        writeFile(Json.encodePrettily(sqlCacheConfig), sqlcaches.resolve(cacheFilePath));
    }

    @Override
    @SneakyThrows
    public void removeSqlCache(String name) {
        String cacheFilePath = getCacheFilePath(name);
        Files.deleteIfExists(sqlcaches.resolve(cacheFilePath));
    }

    @Override
    @SneakyThrows
    public MycatRouterConfig getConfig() {
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

        //BaseMetadataStorageManager.defaultConfig(routerConfig);

        routerConfig.setSchemas(routerConfig.getSchemas().stream().distinct().collect(Collectors.toList()));
        routerConfig.setClusters(routerConfig.getClusters().stream().distinct().collect(Collectors.toList()));
        routerConfig.setDatasources(routerConfig.getDatasources().stream().distinct().collect(Collectors.toList()));
        routerConfig.setUsers(routerConfig.getUsers().stream().distinct().collect(Collectors.toList()));
        routerConfig.setSequences(routerConfig.getSequences().stream().distinct().collect(Collectors.toList()));

        return routerConfig;
    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {

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
    private ConfigReaderWriter getConfigReaderWriter(Path metafile) {
        String fileName = metafile.toString();
        String suffix = getSuffix(fileName);
        return ConfigReaderWriter.getReaderWriterBySuffix(suffix);
    }

    @NotNull
    private String getSuffix(String fileName) {
        return fileName.substring(fileName.lastIndexOf(".") + 1);
    }


    @SneakyThrows
    public void cleanDirectory(Path path) {
        if (Files.exists(path)) {
            org.apache.commons.io.FileUtils.cleanDirectory(path.toFile());
        }
    }


}

