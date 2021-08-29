package io.mycat.config;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DbBaseMetadataStorageManagerImpl implements BaseMetadataStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbBaseMetadataStorageManagerImpl.class);

    final String SCHEMAS_KEY = "SCHEMAS";
    final String CLUSTERS_KEY = "CLUSTERS";
    final String DATASOURCES_KEY = "DATASOURCES";
    final String USERS_KEY = "USERS";
    final String SEQUENCES_KEY = "SEQUENCES";
    final String SQLCACHES_KEY = "SQLCACHES";


    @Override
    @SneakyThrows
    public void putTable(CreateTableConfig createTableConfig) {
        String schemasJson = readConfig(SCHEMAS_KEY).orElseGet(()->Json.encodePrettily(new ShardingQueryRootConfig()));
        ShardingQueryRootConfig shardingQueryRootConfig = Json.decodeValue(schemasJson,ShardingQueryRootConfig.class);

        String schemaName = createTableConfig.getSchemaName();
        String tableName = createTableConfig.getTableName();

        Optional<LogicSchemaConfig> first = shardingQueryRootConfig.getSchemas().stream().filter(i -> i.getSchemaName().equalsIgnoreCase(createTableConfig.getSchemaName()))
                .findFirst();
        LogicSchemaConfig logicSchemaConfig;
        if (first.isPresent()){
            logicSchemaConfig= first.get();
        }else {
            logicSchemaConfig = new LogicSchemaConfig();
            shardingQueryRootConfig.getSchemas().add(logicSchemaConfig);
        }

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

    @SneakyThrows
    public Optional<String> readConfig(String keyText) {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            Map<String, String> config = new HashMap<>();
            Connection rawConnection = connection.getRawConnection();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select `value` from mycat.config where `version` in ( select max(`version`) from mycat.config  group by `key`) and `key` = ?" , Arrays.asList(keyText));
            if (maps.isEmpty()) {
                return Optional.empty();
            }
           return Optional.ofNullable((String) maps.get(0).get("value"));
        }
    }

    @SneakyThrows
    public void writeString(String key,String value){
       long curVersion = System.currentTimeMillis();

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);

        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            Connection rawConnection = connection.getRawConnection();
            rawConnection.setAutoCommit(false);
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement preparedStatement = rawConnection.prepareStatement("INSERT INTO `mycat`.`config` (`key`, `value`, `version`) VALUES (?, ?, ?); ");
            preparedStatement.clearBatch();
            preparedStatement.setObject(1, key);
            preparedStatement.setObject(2, value);
            preparedStatement.setObject(3, curVersion);
            rawConnection.commit();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }
    @SneakyThrows
    public void writeString(Map<String,String> config){
        long curVersion = System.currentTimeMillis();

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);

        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            Connection rawConnection = connection.getRawConnection();
            rawConnection.setAutoCommit(false);
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement preparedStatement = rawConnection.prepareStatement("INSERT INTO `mycat`.`config` (`key`, `value`, `version`) VALUES (?, ?, ?); ");
            preparedStatement.clearBatch();
            for (Map.Entry<String, String> e : config.entrySet()) {
                preparedStatement.setObject(1, e.getKey());
                preparedStatement.setObject(2, e.getValue());
                preparedStatement.setObject(3, curVersion);
                rawConnection.commit();
                rawConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
        }
    }

    @SneakyThrows
    public Map<String, String> readConfig() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            Map<String, String> config = new HashMap<>();
            Connection rawConnection = connection.getRawConnection();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select `key`,`value` from mycat.config where `version` in ( select max(`version`) from mycat.config  group by `key`)  ", Collections.emptyList());
            for (Map<String, Object> map : maps) {
                String key = (String) map.get("key");
                String value = (String) map.get("value");
                config.put(key, value);
            }
            return config;
        }
    }

    void logReplica(int count, LocalDateTime time, Map<String, List<String>> dsNames) {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        if (count > 0) {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
                Connection rawConnection = connection.getRawConnection();
                PreparedStatement preparedStatement = rawConnection.prepareStatement("INSERT INTO `mycat`.`replica_log` (`name`, `dsNames`, `time`) VALUES (?, ?, ?); ");
                for (Map.Entry<String, List<String>> e : dsNames.entrySet()) {
                    String key = e.getKey();
                    String value = String.join(",", e.getValue());
                    preparedStatement.setObject(1, key);
                    preparedStatement.setObject(2, value);
                    preparedStatement.setObject(3, time);
                }
            } catch (Exception e) {
                LOGGER.error("", e);
                vertx.setTimer(TimeUnit.SECONDS.toMillis(3), event -> ioExecutor.executeBlocking((Handler<Promise<Void>>) event1 -> {
                    try {
                        logReplica(count - 1, time, dsNames);
                    } finally {
                        event1.tryComplete();
                    }
                }));
            }
        }
    }


    @Override
    public void removeTable(String schemaNameArg, String tableNameArg) {
        String schemasJson = readConfig(SCHEMAS_KEY).orElseGet(()->Json.encodePrettily(new ShardingQueryRootConfig()));
        ShardingQueryRootConfig shardingQueryRootConfig = Json.decodeValue(schemasJson,ShardingQueryRootConfig.class);

        Optional<LogicSchemaConfig> first = shardingQueryRootConfig.getSchemas().stream().filter(i -> i.getSchemaName().equalsIgnoreCase(schemaNameArg))
                .findFirst();
        first.ifPresent(schemaConfig -> {
            schemaConfig.removeTable(tableNameArg);
            writeString(SCHEMAS_KEY,Json.encodePrettily(shardingQueryRootConfig));
        });
    }

    @Override
    public void putSchema(LogicSchemaConfig schemaConfig) {
        String schemasJson = readConfig(SCHEMAS_KEY).orElseGet(()->Json.encodePrettily(new ShardingQueryRootConfig()));
        ShardingQueryRootConfig shardingQueryRootConfig = Json.decodeValue(schemasJson,ShardingQueryRootConfig.class);
        shardingQueryRootConfig.getSchemas().removeIf(i->i.getSchemaName().equalsIgnoreCase(schemaConfig.getSchemaName()));
        shardingQueryRootConfig.getSchemas().add(schemaConfig);
        writeString(SCHEMAS_KEY,Json.encodePrettily(shardingQueryRootConfig));
    }

    @Override
    public void dropSchema(String schemaName) {
        String schemasJson = readConfig(SCHEMAS_KEY).orElseGet(()->Json.encodePrettily(new ShardingQueryRootConfig()));
        ShardingQueryRootConfig shardingQueryRootConfig = Json.decodeValue(schemasJson,ShardingQueryRootConfig.class);
        shardingQueryRootConfig.getSchemas().removeIf(i->i.getSchemaName().equalsIgnoreCase(schemaName));
        writeString(SCHEMAS_KEY,Json.encodePrettily(shardingQueryRootConfig));
    }

    @Override
    public void putUser(UserConfig userConfig) {
        UserRootConfig userRootConfig = readConfig(USERS_KEY).map(s->Json.decodeValue(s,UserRootConfig.class)).orElseGet(()->new UserRootConfig());
        userRootConfig.getUsers().removeIf(u->u.getUsername().equals(userConfig.getUsername()));
        userRootConfig.getUsers().add(userConfig);
        writeString(USERS_KEY,Json.encodePrettily(userRootConfig));
    }

    @Override
    public void deleteUser(String username) {
        UserRootConfig userRootConfig = readConfig(USERS_KEY).map(s->Json.decodeValue(s,UserRootConfig.class)).orElseGet(()->new UserRootConfig());
        userRootConfig.getUsers().removeIf(u->u.getUsername().equals(username));
        writeString(USERS_KEY,Json.encodePrettily(userRootConfig));
    }

    @Override
    public void putSequence(SequenceConfig sequenceConfig) {
        Sequence sequence = readConfig(USERS_KEY).map(s->Json.decodeValue(s,Sequence.class)).orElseGet(()->new Sequence());
        sequence.getSequences().removeIf(s->s.getName().equalsIgnoreCase(sequenceConfig.getName()));
        sequence.getSequences().add(sequenceConfig);
        writeString(SEQUENCES_KEY,Json.encodePrettily(sequence));
    }

    @Override
    public void removeSequenceByName(String name) {
        Sequence sequence = readConfig(SEQUENCES_KEY).map(s->Json.decodeValue(s,Sequence.class)).orElseGet(()->new Sequence());
        sequence.getSequences().removeIf(s->s.getName().equalsIgnoreCase(name));
        writeString(SEQUENCES_KEY,Json.encodePrettily(sequence));
    }

    @Override
    public void putDatasource(DatasourceConfig datasourceConfig) {
        DatasourceRootConfig datasourceRootConfig = readConfig(DATASOURCES_KEY).map(s->Json.decodeValue(s,DatasourceRootConfig.class)).orElseGet(()->new DatasourceRootConfig());
        datasourceRootConfig.getDatasources().removeIf(s->s.getName().equalsIgnoreCase(datasourceConfig.getName()));
        datasourceRootConfig.getDatasources().add(datasourceConfig);
        writeString(DATASOURCES_KEY,Json.encodePrettily(datasourceRootConfig));
    }

    @Override
    public void removeDatasource(String datasourceName) {
        DatasourceRootConfig datasourceRootConfig = readConfig(DATASOURCES_KEY).map(s->Json.decodeValue(s,DatasourceRootConfig.class)).orElseGet(()->new DatasourceRootConfig());
        datasourceRootConfig.getDatasources().removeIf(s->s.getName().equalsIgnoreCase(datasourceName));
        writeString(DATASOURCES_KEY,Json.encodePrettily(datasourceRootConfig));
    }

    @Override
    public void putReplica(ClusterConfig clusterConfig) {
        ClusterRootConfig clusterRootConfig = readConfig(DATASOURCES_KEY).map(s->Json.decodeValue(s,ClusterRootConfig.class)).orElseGet(()->new ClusterRootConfig());
        clusterRootConfig.getClusters().removeIf(c->c.getName().equalsIgnoreCase(clusterConfig.getName()));
        clusterRootConfig.getClusters().add(clusterConfig);
        writeString(CLUSTERS_KEY,Json.encodePrettily(clusterRootConfig));
    }

    @Override
    public void removeReplica(String replicaName) {
        ClusterRootConfig clusterRootConfig = readConfig(DATASOURCES_KEY).map(s->Json.decodeValue(s,ClusterRootConfig.class)).orElseGet(()->new ClusterRootConfig());
        clusterRootConfig.getClusters().removeIf(c->c.getName().equalsIgnoreCase(replicaName));
        writeString(CLUSTERS_KEY,Json.encodePrettily(clusterRootConfig));
    }

    @Override
    public void sync(MycatRouterConfig mycatRouterConfig) {
           List<LogicSchemaConfig>  schemas = mycatRouterConfig.getSchemas();
          List<ClusterConfig> clusters   = mycatRouterConfig.getClusters();
         List<DatasourceConfig> datasources = mycatRouterConfig.getDatasources();
         List<UserConfig> users =  mycatRouterConfig.getUsers();
         List<SequenceConfig> sequences = mycatRouterConfig.getSequences();
         List<SqlCacheConfig> sqlCacheConfigs = mycatRouterConfig.getSqlCacheConfigs();

        ShardingQueryRootConfig shardingQueryRootConfig = new ShardingQueryRootConfig();
        shardingQueryRootConfig.setSchemas(schemas);

        ClusterRootConfig clusterRootConfig = new ClusterRootConfig();
        clusterRootConfig.setClusters(clusters);

        DatasourceRootConfig datasourceRootConfig = new DatasourceRootConfig();
        datasourceRootConfig.setDatasources(datasources);

        UserRootConfig userRootConfig = new UserRootConfig();
        userRootConfig.setUsers(users);

        Sequence sequence = new Sequence();
        sequence.setSequences(sequences);

        SqlCacheRootConfig sqlCacheRootConfig = new SqlCacheRootConfig();
        sqlCacheRootConfig.setSqlCaches(sqlCacheConfigs);


        HashMap<String,String> map = new HashMap<>();
        map.put(SCHEMAS_KEY,Json.encodePrettily(shardingQueryRootConfig));
        map.put(CLUSTERS_KEY,Json.encodePrettily(clusterRootConfig));
        map.put(DATASOURCES_KEY,Json.encodePrettily(datasourceRootConfig));
        map.put(USERS_KEY,Json.encodePrettily(userRootConfig));
        map.put(SEQUENCES_KEY,Json.encodePrettily(sequence));
        map.put(SQLCACHES_KEY,Json.encodePrettily(sqlCacheRootConfig));

        writeString(map);
    }

    @Override
    public void putSqlCache(SqlCacheConfig sqlCacheConfig) {
        SqlCacheRootConfig sqlCacheRootConfig = readConfig(SQLCACHES_KEY).map(s->Json.decodeValue(s,SqlCacheRootConfig.class)).orElseGet(()->new SqlCacheRootConfig());
        sqlCacheRootConfig.getSqlCaches().removeIf(c->c.getName().equalsIgnoreCase(sqlCacheConfig.getName()));
        sqlCacheRootConfig.getSqlCaches().add(sqlCacheConfig);
        writeString(SQLCACHES_KEY,Json.encodePrettily(sqlCacheRootConfig));
    }

    @Override
    public void removeSqlCache(String name) {
        SqlCacheRootConfig sqlCacheRootConfig = readConfig(SQLCACHES_KEY).map(s->Json.decodeValue(s,SqlCacheRootConfig.class)).orElseGet(()->new SqlCacheRootConfig());
        sqlCacheRootConfig.getSqlCaches().removeIf(c->c.getName().equalsIgnoreCase(name));
        writeString(SQLCACHES_KEY,Json.encodePrettily(sqlCacheRootConfig));
    }

    @Override
    public MycatRouterConfig getConfig() {
        Map<String, String> map = readConfig();
        final String SCHEMAS_KEY = "SCHEMAS";
        final String CLUSTERS_KEY = "CLUSTERS";
        final String DATASOURCES_KEY = "DATASOURCES";
        final String USERS_KEY = "USERS";
        final String SEQUENCES_KEY = "SEQUENCES";
        final String SQLCACHES_KEY = "SQLCACHES";


        String schemaConfig = map.get(SCHEMAS_KEY);
        ShardingQueryRootConfig shardingQueryRootConfig;
        if (schemaConfig != null){
            shardingQueryRootConfig = Json.decodeValue(schemaConfig, ShardingQueryRootConfig.class);
        }else {
            shardingQueryRootConfig =  new ShardingQueryRootConfig();
        }
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String clusterConfig = map.get(CLUSTERS_KEY);
        ClusterRootConfig clusterRootConfig;
        if (clusterConfig != null){
            clusterRootConfig =  Json.decodeValue(clusterConfig, ClusterRootConfig.class);
        }else {
            clusterRootConfig = new ClusterRootConfig();
        }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String datasourceConfig = map.get(DATASOURCES_KEY);
        DatasourceRootConfig datasourceRootConfig;
        if (datasourceConfig != null){
            datasourceRootConfig =  Json.decodeValue(datasourceConfig, DatasourceRootConfig.class);
        }else {
            datasourceRootConfig = new DatasourceRootConfig();
        }
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String userConfig = map.get(USERS_KEY);
        UserRootConfig userRootConfig;

        if (userConfig != null){
            userRootConfig =  Json.decodeValue(userConfig, UserRootConfig.class);
        }else {
            userRootConfig = new UserRootConfig();
        }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String sequenceConfig = map.get(SEQUENCES_KEY);
        Sequence sequence;
        if (sequenceConfig != null){
            sequence =  Json.decodeValue(sequenceConfig, Sequence.class);
        }else {
            sequence = new Sequence();
        }
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        String sqlcacheConfig = map.get(SQLCACHES_KEY);
        SqlCacheRootConfig sqlCacheRootConfig;
        if (sqlcacheConfig!=null){
            sqlCacheRootConfig =  Json.decodeValue(sqlcacheConfig, SqlCacheRootConfig.class);
        }else {
            sqlCacheRootConfig = new SqlCacheRootConfig();
        }

        MycatRouterConfig mycatRouterConfig = new MycatRouterConfig();
        mycatRouterConfig.setSchemas(shardingQueryRootConfig.getSchemas());
        mycatRouterConfig.setClusters(clusterRootConfig.getClusters());
        mycatRouterConfig.setDatasources(datasourceRootConfig.getDatasources());
        mycatRouterConfig.setSequences(sequence.getSequences());
        mycatRouterConfig.setSqlCacheConfigs(sqlCacheRootConfig.getSqlCaches());

        return mycatRouterConfig;
    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {
        logReplica(3, LocalDateTime.now(), dsNames);
    }

    @Data
    public static class UserRootConfig {
        List<UserConfig> users = new ArrayList<>();
    }

    @Data
    public static class SqlCacheRootConfig {
        List<SqlCacheConfig> sqlCaches = new ArrayList<>();
    }


}
