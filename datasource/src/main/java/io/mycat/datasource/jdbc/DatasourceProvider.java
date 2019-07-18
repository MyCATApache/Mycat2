package io.mycat.datasource.jdbc;

import java.util.Map;
import javax.sql.DataSource;

public interface DatasourceProvider {
  public DataSource createDataSource(JdbcDataSource config, Map<String, String> jdbcDriverMap);
}
