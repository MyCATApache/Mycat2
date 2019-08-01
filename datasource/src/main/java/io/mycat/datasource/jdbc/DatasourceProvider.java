package io.mycat.datasource.jdbc;

import java.util.Map;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

public interface DatasourceProvider {

  DataSource createDataSource(JdbcDataSource config, Map<String, String> jdbcDriverMap);

  default boolean isJTA() {
    return false;
  }

  default UserTransaction createUserTransaction() {
    throw new UnsupportedOperationException();
  }
}
