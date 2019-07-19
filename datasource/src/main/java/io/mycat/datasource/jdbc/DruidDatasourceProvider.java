package io.mycat.datasource.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

public class DruidDatasourceProvider implements DatasourceProvider {
  @Override
  public DataSource createDataSource(JdbcDataSource config, Map<String, String> jdbcDriverMap) {
    String password = config.getPassword();
    String username = config.getUsername();
    String url = config.getUrl();
    String dbType = config.getDbType();
    String db = config.getDb();
    String jdbcDriver = jdbcDriverMap.get(dbType);

    DruidDataSource datasource = new DruidDataSource();
    datasource.setPassword(password);
    datasource.setUsername(username);
    datasource.setUrl(url);
    datasource.setDriverClassName(jdbcDriver);
    datasource.setMaxWait(TimeUnit.MILLISECONDS.toMillis(500));
    return datasource;
  }
}