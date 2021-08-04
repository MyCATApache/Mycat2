package io.mycat.ui;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.LogicSchemaConfig;
import io.mycat.config.MycatRouterConfig;
import io.mycat.hint.*;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class TcpInfoProvider implements InfoProvider {
    private final DruidDataSource druidDataSource;
    public String url;
    public String user;
    public String password;

    AtomicBoolean needUpdateFlag = new AtomicBoolean();
    MycatRouterConfig config;

    public TcpInfoProvider(
            String url,
            String user,
            String password
    ) {
        this.druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(user);
        druidDataSource.setPassword(password);
    }

//     <T> T write(Supplier<T> runnable){
//        needUpdateFlag.set(true);
//      return  runnable.get();
//    }
//    <T> T  read(Supplier<T> runnable){
//      if (needUpdateFlag.get()){
//          config = getMycatRouterConfig()
//      }
//       return runnable.get();
//    }

    @Override
    @SneakyThrows
    public List<LogicSchemaConfig> schemas() {
        MycatRouterConfig routerConfig = getMycatRouterConfig();
        return routerConfig.getSchemas();
    }

    @SneakyThrows
    private MycatRouterConfig getMycatRouterConfig() {
        List<Map<String, Object>> maps = JdbcUtils.executeQuery(this.druidDataSource, new ShowConfigTextHint().build());
        String text = (String) maps.get(0).values().iterator().next();
        return Json.decodeValue(text, MycatRouterConfig.class);
    }

    @Override
    public List<ClusterConfig> clusters() {
        MycatRouterConfig routerConfig = getMycatRouterConfig();
        return routerConfig.getClusters();
    }

    @Override
    public List<DatasourceConfig> datasources() {
        MycatRouterConfig routerConfig = getMycatRouterConfig();
        return routerConfig.getDatasources();
    }

    @Override
    public Optional<LogicSchemaConfig> getSchemaConfigByName(String schemaName) {
        MycatRouterConfig routerConfig = getMycatRouterConfig();
        return routerConfig.getSchemas().stream().filter(i -> i.getSchemaName().equalsIgnoreCase(schemaName)).findFirst();
    }

    @Override
    public Optional<Object> getTableConfigByName(String schemaName, String tableName) {
        MycatRouterConfig routerConfig = getMycatRouterConfig();
        return routerConfig.getSchemas().stream().filter(config -> config.getSchemaName().equalsIgnoreCase(schemaName)).findFirst().flatMap(i -> i.findTable(tableName));
    }

    @Override
    public Optional<DatasourceConfig> getDatasourceConfigByPath(String name) {
        MycatRouterConfig routerConfig = getMycatRouterConfig();
        return routerConfig.getDatasources().stream().filter(i -> i.getName().equalsIgnoreCase(name)).findFirst();
    }

    @Override
    public Optional<ClusterConfig> getClusterConfigByPath(String name) {
        MycatRouterConfig routerConfig = getMycatRouterConfig();
        return routerConfig.getClusters().stream().filter(i -> i.getName().equalsIgnoreCase(name)).findFirst();
    }

    @Override
    public String translate(String name) {
        return name;
    }

    @Override
    @SneakyThrows
    public void deleteDatasource(String datasource) {
        JdbcUtils.execute(this.druidDataSource, DropDataSourceHint.create(datasource));
    }

    @Override
    @SneakyThrows
    public void deleteLogicalSchema(String schema) {
        JdbcUtils.execute(this.druidDataSource, DropSchemaHint.create(schema));
    }

    @Override
    @SneakyThrows
    public void saveCluster(ClusterConfig config) {
        JdbcUtils.execute(this.druidDataSource, CreateClusterHint.create(config));
    }

    @Override
    @SneakyThrows
    public void saveDatasource(DatasourceConfig config) {
        JdbcUtils.execute(this.druidDataSource, CreateDataSourceHint.create(config));
    }

    @Override
    @SneakyThrows
    public Connection createConnection() {
        return this.druidDataSource.getConnection();
    }
}