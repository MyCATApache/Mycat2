package io.mycat.sqlhandler.config;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.mycat.MetaClusterCurrent;
import io.mycat.SQLInits;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.curator.shaded.com.google.common.io.Files;

import java.io.File;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class DbStorageManagerImpl extends AbstractStorageManagerImpl {

    final DatasourceConfig config;
    final static ConcurrentMap<Object, Object> CACHE = new ConcurrentHashMap<>();

    @SneakyThrows
    public DbStorageManagerImpl(DatasourceConfig config) {
        this.config = config;

        if (!CACHE.containsKey(config.getName())) {
            try (Ds ds = Ds.create(config);
                 Connection rawConnection = ds.getConnection()) {
                List<Map<String, Object>> show_databases = JdbcUtils.executeQuery(rawConnection, "show databases", Collections.emptyList());
                boolean isPresent = show_databases.stream().filter(i -> "mycat".equalsIgnoreCase((String) i.get("Database"))).findFirst().isPresent();
                if (!isPresent){
                    URL resource = SQLInits.class.getResource("/mycat2init.sql");
                    File file = new File(resource.toURI());
                    String s = new String(Files.toByteArray(file));
                    for (SQLStatement parseStatement : SQLUtils.parseStatements(s, DbType.mysql)) {
                        JdbcUtils.execute(rawConnection, parseStatement.toString());
                    }
                }

            }
            CACHE.put(config.getName(), Boolean.TRUE);
        }

    }

    @Override
    public <T extends KVObject> KV<T> get(String path, String fileNameTemplate, Class<T> aClass) {
        return new DbKVImpl(config, path, aClass);
    }


    @Override
    public void syncFromNet() {
    }

    @Override
    public void syncToNet() {

    }

    @Override
    public boolean checkConfigConsistency() {
        return true;
    }

    @Override
    @SneakyThrows
    public void reportReplica(Map<String, List<String>> state) {
        LocalDateTime time = LocalDateTime.now();
        try (Ds ds = Ds.create(config);
             Connection rawConnection = ds.getConnection()) {
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);
            PreparedStatement preparedStatement = rawConnection.prepareStatement("INSERT INTO `mycat`.`replica_log` (`name`, `dsNames`, `time`) VALUES (?, ?, ?); ");
            for (Map.Entry<String, List<String>> e : state.entrySet()) {
                String key = e.getKey();
                String value = String.join(",", e.getValue());
                preparedStatement.setObject(1, key);
                preparedStatement.setObject(2, value);
                preparedStatement.setObject(3, time);
            }
            preparedStatement.execute();
            rawConnection.commit();
        }
    }

    @AllArgsConstructor
    static class Ds implements AutoCloseable {
        DatasourceConfig datasourceConfig;
        JdbcDataSource jdbcDataSource;
        boolean tmpDataSource = false;

        public static Ds create(DatasourceConfig datasourceConfig) {
            JdbcDataSource jdbcDataSource = null;
            boolean tmpDataSource = false;
            if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
                JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                Map<String, JdbcDataSource> datasourceInfo = jdbcConnectionManager.getDatasourceInfo();
                if (datasourceInfo.containsKey(datasourceConfig.getName())) {
                    jdbcDataSource = datasourceInfo.get(datasourceConfig.getName());
                }
            }
            if (jdbcDataSource == null) {
                DruidDatasourceProvider druidDatasourceProvider = new DruidDatasourceProvider();
                jdbcDataSource = druidDatasourceProvider.createDataSource(datasourceConfig);
                tmpDataSource = true;
            }
            return new Ds(datasourceConfig, jdbcDataSource, tmpDataSource);
        }

        @Override
        public void close() throws Exception {
            if (tmpDataSource) {
                jdbcDataSource.close();
            }
        }

        @SneakyThrows
        public Connection getConnection() {
            return jdbcDataSource.getDataSource().getConnection();
        }
    }

    @SneakyThrows
    public static Config readConfig(DatasourceConfig datasourceConfig) {
        Config config = new Config();
        try (Ds ds = Ds.create(datasourceConfig);
             Connection rawConnection = ds.getConnection()) {
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select `key`,`secondKey`,`value`,`version` from mycat.config where `deleted` = 0 and `version` in ( select max(`version`) from mycat.config  group by `key`)  ", Collections.emptyList());
            for (Map<String, Object> map : maps) {
                String key = (String) Objects.toString(map.get("key"));
                String key2 = (String) Objects.toString(map.get("secondKey"));
                String value = (String) Objects.toString(map.get("value"));
                config.version = Long.parseLong((String) Objects.toString(map.get("version")));
                Map<String, String> stringStringMap = config.config.computeIfAbsent(key, s -> new HashMap<>());
                stringStringMap.put(key2, value);
            }
        }
        return config;
    }

    @SneakyThrows
    public static void writeString(DatasourceConfig datasourceConfig, String key, String key2, String value) {
        long curVersion = System.currentTimeMillis();
        try (Ds ds = Ds.create(datasourceConfig);
             Connection rawConnection = ds.getConnection()) {
            rawConnection.setAutoCommit(false);
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement preparedStatement = rawConnection.prepareStatement(
                    "INSERT INTO `mycat`.`config` (`key`,`secondKey` ,`value`, `version`) VALUES (?,?, ?, ?); ");
            preparedStatement.clearBatch();
            preparedStatement.setObject(1, key);
            preparedStatement.setObject(2, key2);
            preparedStatement.setObject(3, value);
            preparedStatement.setObject(4, curVersion);
            rawConnection.commit();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    @SneakyThrows
    public static void writeString(DatasourceConfig datasourceConfig, Map<String, Map<String, String>> config) {
        long curVersion = System.currentTimeMillis();
        try (Ds ds = Ds.create(datasourceConfig);
             Connection rawConnection = ds.getConnection()) {
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);

            for (Map.Entry<String, Map<String, String>> e : config.entrySet()) {
                String key = e.getKey();
                Map<String, String> map = e.getValue();
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    JdbcUtils.execute(rawConnection, "INSERT INTO `mycat`.`config` (`key`,`secondKey`, `value`, `version`) VALUES (?,?, ?, ?); ",
                            Arrays.asList(e.getKey(), entry.getKey(), entry.getValue(), curVersion));
                }
            }

            rawConnection.commit();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    @SneakyThrows
    public static void removeBy(DatasourceConfig datasourceConfig, long curVersion) {
        try (Ds ds = Ds.create(datasourceConfig);
             Connection rawConnection = ds.getConnection()) {
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);
            JdbcUtils.execute(rawConnection, "update  `mycat`.`config` set deleted = 1 where version = ?; ",
                    Arrays.asList(curVersion));

            rawConnection.commit();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }
}
