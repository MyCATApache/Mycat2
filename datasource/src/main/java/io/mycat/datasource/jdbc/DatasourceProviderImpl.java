package io.mycat.datasource.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.mycat.datasource.jdbc.JdbcDataSourceManager.DatasourceProvider;
import java.io.PrintWriter;
import java.util.Properties;
import javax.sql.DataSource;

public enum DatasourceProviderImpl implements DatasourceProvider {
  INSTANCE;

  @Override
  public DataSource createDataSource(String url, String username, String password) {
    Properties props = new Properties();
    props.setProperty("dataSourceClassName", "org.mariadb.jdbc.MariaDbDataSource");
    props.put("dataSource.logWriter", new PrintWriter(System.out));
    HikariConfig config = new HikariConfig(props);
    config.setJdbcUrl(url);
    config.setUsername(username);
    config.setPassword(password);
    return new HikariDataSource(config);
  }
}