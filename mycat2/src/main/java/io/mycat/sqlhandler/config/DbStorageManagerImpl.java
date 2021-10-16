package io.mycat.sqlhandler.config;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.KVObject;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbStorageManagerImpl extends AbstractStorageManagerImpl {

  final   DatasourceConfig config;

    public DbStorageManagerImpl(DatasourceConfig config) {
        this.config = config;
    }

    @Override
    public <T extends KVObject> KV<T> get(String path, String fileNameTemplate, Class<T> aClass) {
      return new DbKVImpl(config,path,aClass);
    }


    @Override
    public void syncFromNet() {
        }

    @Override
    public void syncToNet() {

    }

    @Override
    @SneakyThrows
    public void reportReplica(Map<String, List<String>> state) {
        LocalDateTime time = LocalDateTime.now();
        try(Ds ds = Ds.create(config);
            Connection rawConnection = ds.getConnection()){
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
        public Connection getConnection(){
            return jdbcDataSource.getDataSource().getConnection();
        }
    }

    @SneakyThrows
    public  static Config readConfig(DatasourceConfig datasourceConfig) {
        Config config = new Config();
        try(Ds ds = Ds.create(datasourceConfig);
            Connection rawConnection = ds.getConnection()){
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            rawConnection.setAutoCommit(false);
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select `key`,`secondKey`,`value`,`version` from mycat.config where `version` in ( select max(`version`) from mycat.config  group by `key`)  ", Collections.emptyList());
            for (Map<String, Object> map : maps) {
                String key = (String) map.get("key");
                String key2 = (String) map.get("secondKey");
                String value = (String) map.get("value");
                config.version = Long.parseLong((String) map.get("version"));
                Map<String, String> stringStringMap = config.config.computeIfAbsent(key, s -> new HashMap<>());
                stringStringMap.put(key2,value);
            }
        }
        return config;
    }

    @SneakyThrows
    public  static void writeString(DatasourceConfig datasourceConfig, String key,String key2, String value) {
        long curVersion = System.currentTimeMillis();
        try(Ds ds = Ds.create(datasourceConfig);
            Connection rawConnection = ds.getConnection()){
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
    public static void writeString(DatasourceConfig datasourceConfig,Map<String, Map<String,String>> config) {
        long curVersion = System.currentTimeMillis();
        try(Ds ds = Ds.create(datasourceConfig);
            Connection rawConnection = ds.getConnection()){
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement preparedStatement = rawConnection
                    .prepareStatement("INSERT INTO `mycat`.`config` (`key`,`secondKey`, `value`, `version`) VALUES (?,?, ?, ?); ");
            preparedStatement.clearBatch();
            for (Map.Entry<String,  Map<String,String>> e : config.entrySet()) {
                String key = e.getKey();
                Map<String, String> map = e.getValue();
                for (Map.Entry<String, String> entry : map.entrySet()) {

                    preparedStatement.setObject(1, e.getKey());
                    preparedStatement.setObject(2, entry.getKey());
                    preparedStatement.setObject(3, entry.getValue());
                    preparedStatement.setObject(4, curVersion);
                    preparedStatement.addBatch();
                }
            }
            rawConnection.commit();
            rawConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
    }
}
