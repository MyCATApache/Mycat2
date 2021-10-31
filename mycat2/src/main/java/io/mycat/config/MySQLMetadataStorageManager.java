//package io.mycat.config;
//
//import com.alibaba.druid.util.JdbcUtils;
//import io.mycat.ConfigOps;
//import io.mycat.IOExecutor;
//import io.mycat.MetaClusterCurrent;
//import io.mycat.MetadataStorageManager;
//import io.mycat.datasource.jdbc.datasource.DefaultConnection;
//import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
//import io.mycat.util.NameMap;
//import io.vertx.core.Handler;
//import io.vertx.core.Promise;
//import io.vertx.core.Vertx;
//import io.vertx.core.json.Json;
//import lombok.Data;
//import lombok.SneakyThrows;
//import org.jetbrains.annotations.Nullable;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//
//public class MySQLMetadataStorageManager extends MetadataStorageManager {
//    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLMetadataStorageManager.class);
//
//    final static String SCHEMAS_KEY = "SCHEMAS";
//    final String CLUSTERS_KEY = "CLUSTERS";
//    final String DATASOURCES_KEY = "DATASOURCES";
//    final String USERS_KEY = "USERS";
//    final String SEQUENCES_KEY = "SEQUENCES";
//    final String SQLCACHES_KEY = "SQLCACHES";
//    long curVersion = -1;
//
//    public MySQLMetadataStorageManager() {
//
//    }
//
//    @Override
//    public void start() throws Exception {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void start(MycatRouterConfig mycatRouterConfig) {
//        throw new UnsupportedOperationException();
//    }
//
//    @SneakyThrows
//    public MycatRouterConfig fetchFromStore() {
//        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//
//
//        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
//            Map<String, String> config = new HashMap<>();
//            Connection rawConnection = connection.getRawConnection();
//            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
//            rawConnection.setAutoCommit(false);
//            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select * from mycat.config where id in ( select max(id) from mycat.config  group by `key`)  ", Collections.emptyList());
//            for (Map<String, Object> map : maps) {
//                String key = (String) map.get("key");
//                String value = (String) map.get("value");
//                Number version = (Number) map.get("version");
//                config.put(key, value);
//                curVersion = Math.max(curVersion, version.longValue());
//            }
//
//
//            NameMap nameMap = NameMap.immutableCopyOf(config);
////            String serverText = (String) nameMap.get("SERVER", false);
//            String schemasText = (String) nameMap.get(SCHEMAS_KEY, false);
//            String clustersText = (String) nameMap.get(CLUSTERS_KEY, false);
//            String datasourcesText = (String) nameMap.get(DATASOURCES_KEY, false);
//            String usersText = (String) nameMap.get(USERS_KEY, false);
//            String sequencesText = (String) nameMap.get(SEQUENCES_KEY, false);
//            String sqlCacheText = (String) nameMap.get(SQLCACHES_KEY, false);
//            MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
////            if (serverText == null) {
////                serverText = Json.encode(MetaClusterCurrent.wrapper(ServerConfig.class));
////            }
//            if (schemasText == null) {
//                schemasText = getSchemaJson(mycatRouterConfig);
//            }
//            if (clustersText == null) {
//                clustersText = getClusterJson(mycatRouterConfig);
//            }
//            if (datasourcesText == null) {
//                datasourcesText = getDatasourceJson(mycatRouterConfig);
//            }
//            if (usersText == null) {
//                usersText = getUserJson(mycatRouterConfig);
//            }
//            if (sequencesText == null) {
//                sequencesText = getSequenceJson(mycatRouterConfig);
//            }
//            if (sqlCacheText == null) {
//                sqlCacheText = getSqlCacheJson(mycatRouterConfig);
//            }
////            ServerConfig serverConfig = Json.decodeValue(serverText, ServerConfig.class);
//            ShardingQueryRootConfig schemasConfig = Json.decodeValue(schemasText, ShardingQueryRootConfig.class);
//            ClusterRootConfig clustersConfig = Json.decodeValue(clustersText, ClusterRootConfig.class);
//            DatasourceRootConfig datasourcesConfig = Json.decodeValue(datasourcesText, DatasourceRootConfig.class);
//            UserRootConfig usersConfig = Json.decodeValue(usersText, UserRootConfig.class);
//            Sequence sequence = Json.decodeValue(sequencesText, Sequence.class);
//            SqlCacheRootConfig sqlCacheRootConfig = Json.decodeValue(sqlCacheText, SqlCacheRootConfig.class);
//
//
//            MycatRouterConfig newMycatRouterConfig = new MycatRouterConfig();
//            newMycatRouterConfig.setSchemas(schemasConfig.getSchemas());
//            newMycatRouterConfig.setClusters(clustersConfig.getClusters());
//            newMycatRouterConfig.setDatasources(datasourcesConfig.getDatasources());
//            newMycatRouterConfig.setUsers(usersConfig.getUsers());
//            newMycatRouterConfig.setSqlCacheConfigs(sqlCacheRootConfig.getSqlCaches());
//            newMycatRouterConfig.setSequences(sequence.getSequences());
//            return newMycatRouterConfig;
//        }
//    }
//
//    @Override
//    @SneakyThrows
//    public void sync(MycatRouterConfig mycatRouterConfig, State state) {
//        String schemasText = getSchemaJson(mycatRouterConfig);
//        String clustersText = getClusterJson(mycatRouterConfig);
//        String datasourcesText = getDatasourceJson(mycatRouterConfig);
//        String usersText = getUserJson(mycatRouterConfig);
//        String sequencesText = getSequenceJson(mycatRouterConfig);
//        String sqlCacheText = getSqlCacheJson(mycatRouterConfig);
//
//        HashMap<String, String> map = new HashMap<>();
//        map.put(SCHEMAS_KEY, schemasText);
//        map.put(CLUSTERS_KEY, clustersText);
//        map.put(DATASOURCES_KEY, datasourcesText);
//        map.put(USERS_KEY, usersText);
//        map.put(SEQUENCES_KEY, sequencesText);
//        map.put(SQLCACHES_KEY, sqlCacheText);
//
//        curVersion = System.currentTimeMillis();
//
//        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//
//
//        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
//            Connection rawConnection = connection.getRawConnection();
//            rawConnection.setAutoCommit(false);
//            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
//            PreparedStatement preparedStatement = rawConnection.prepareStatement("INSERT INTO `mycat`.`config` (`key`, `value`, `version`) VALUES (?, ?, ?); ");
//            preparedStatement.clearBatch();
//            for (Map.Entry<String, String> e : map.entrySet()) {
//                preparedStatement.setObject(1, e.getKey());
//                preparedStatement.setObject(2, e.getValue());
//                preparedStatement.setObject(3, curVersion);
//            }
//            rawConnection.commit();
//            rawConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
//        }
//
//    }
//
//    private String getSqlCacheJson(MycatRouterConfig mycatRouterConfig) {
//        List<SqlCacheConfig> sqlCacheConfigs = mycatRouterConfig.getSqlCacheConfigs();
//        SqlCacheRootConfig sqlCacheRootConfig = new SqlCacheRootConfig();
//        sqlCacheRootConfig.setSqlCaches(sqlCacheConfigs);
//        return Json.encode(sqlCacheRootConfig);
//    }
//
//    private String getSequenceJson(MycatRouterConfig mycatRouterConfig) {
//        List<SequenceConfig> sequences = mycatRouterConfig.getSequences();
//        Sequence sequence = new Sequence();
//        sequence.setSequences(sequences);
//        return Json.encode(sequence);
//    }
//
//    private String getUserJson(MycatRouterConfig mycatRouterConfig) {
//        String usersText;
//        UserRootConfig userRootConfig = new UserRootConfig();
//        userRootConfig.setUsers(mycatRouterConfig.getUsers());
//        usersText = Json.encode(userRootConfig);
//        return usersText;
//    }
//
//    private String getDatasourceJson(MycatRouterConfig mycatRouterConfig) {
//        String datasourcesText;
//        DatasourceRootConfig datasourceRootConfig = new DatasourceRootConfig();
//        datasourceRootConfig.setDatasources(mycatRouterConfig.getDatasources());
//        datasourcesText = Json.encode(datasourceRootConfig);
//        return datasourcesText;
//    }
//
//    private String getClusterJson(MycatRouterConfig mycatRouterConfig) {
//        ClusterRootConfig clusterConfig = new ClusterRootConfig();
//        clusterConfig.setClusters(mycatRouterConfig.getClusters());
//        return Json.encode(clusterConfig);
//    }
//
//    private String getSchemaJson(MycatRouterConfig mycatRouterConfig) {
//        String schemasText;
//        ShardingQueryRootConfig schemasConfig = new ShardingQueryRootConfig();
//        schemasConfig.schemas = mycatRouterConfig.getSchemas();
//        schemasConfig.prototype = "prototype";
//        schemasText = Json.encode(schemasConfig);
//        return schemasText;
//    }
//
//    @Override
//    public void reportReplica(Map<String, List<String>> dsNames) {
//        logReplica(3, LocalDateTime.now(), dsNames);
//    }
//
//    @Nullable
//    @Override
//    public Optional<DatasourceConfig> getPrototypeDatasourceConfig() {
//        return Optional.empty();
//    }
//
//    void logReplica(int count, LocalDateTime time, Map<String, List<String>> dsNames) {
//        State state = new State();
//        state.getReplica().putAll(dsNames);
//        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//        Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
//        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
//        if (count > 0) {
//            try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
//                Connection rawConnection = connection.getRawConnection();
//                PreparedStatement preparedStatement = rawConnection.prepareStatement("INSERT INTO `mycat`.`replica_log` (`name`, `dsNames`, `time`) VALUES (?, ?, ?); ");
//                for (Map.Entry<String, List<String>> e : dsNames.entrySet()) {
//                    String key = e.getKey();
//                    String value = String.join(",", e.getValue());
//                    preparedStatement.setObject(1, key);
//                    preparedStatement.setObject(2, value);
//                    preparedStatement.setObject(3, time);
//                }
//            } catch (Exception e) {
//                LOGGER.error("", e);
//                vertx.setTimer(TimeUnit.SECONDS.toMillis(3), event -> ioExecutor.executeBlocking((Handler<Promise<Void>>) event1 -> {
//                    try {
//                        logReplica(count - 1, time, dsNames);
//                    } finally {
//                        event1.tryComplete();
//                    }
//                }));
//            }
//        }
//    }
//
//    @Override
//    public ConfigOps startOps() {
//        throw new UnsupportedOperationException();
//    }
//
//
//    @Data
//    public static class UserRootConfig {
//        List<UserConfig> users = new ArrayList<>();
//    }
//
//    @Data
//    public static class SqlCacheRootConfig {
//        List<SqlCacheConfig> sqlCaches = new ArrayList<>();
//    }
//}
