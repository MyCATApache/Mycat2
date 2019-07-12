package io.mycat.datasource.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import io.mycat.datasource.jdbc.JdbcDataSourceManager.DatasourceProvider;
import javax.sql.DataSource;

public enum DatasourceProviderImpl implements DatasourceProvider {
  INSTANCE;

  @Override
  public DataSource createDataSource(String url, String username, String password) {
//    Properties props = new Properties();
//    props.setProperty("dataSourceClassName",);
//    props.put("dataSource.logWriter", new PrintWriter(System.out));
//    HikariConfig config = new HikariConfig(props);
//    config.setJdbcUrl(url);
//    config.setSchema("db1");
//    config.setConnectionInitSql("use db1;");
//    config.setUsername(username);
//    config.setPassword(password);

    DruidDataSource datasource = new DruidDataSource();
    datasource.setUrl(url);
    datasource.setUsername(username);
    datasource.setPassword(password);
    datasource.setDriverClassName("com.mysql.jdbc.Driver");
    return datasource;
  }
}