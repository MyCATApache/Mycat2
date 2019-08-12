package io.mycat.datasource.jdbc.datasourceProvider;

import com.alibaba.druid.pool.DruidDataSource;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

public class DruidDatasourceProvider implements DatasourceProvider {
  @Override
  public DataSource createDataSource(JdbcDataSource config, Map<String, String> jdbcDriverMap) {
    String username = config.getUsername();
    String password = config.getPassword();
    String url = config.getUrl();
    String dbType = config.getDbType();
    String db = config.getDb();
    String jdbcDriver = jdbcDriverMap.get(dbType);

    DruidDataSource datasource = new DruidDataSource();
    datasource.setPassword(password);
    datasource.setUsername(username);
    datasource.setUrl(url);
    datasource.setDriverClassName(jdbcDriver);
    datasource.setMaxWait(TimeUnit.MILLISECONDS.toMillis(300));
    return datasource;
  }
}