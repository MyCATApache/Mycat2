package io.mycat.datasource.jdbc.datasourceProvider;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.JdbcDataSource;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

public class AtomikosDatasourceProvider implements DatasourceProvider {

  @Override
  public DataSource createDataSource(JdbcDataSource config, Map<String, String> jdbcDriverMap) {
    String password = config.getPassword();
    String username = config.getUsername();
    String url = config.getUrl();
    String dbType = config.getDbType();
    String db = config.getDb();
    String jdbcDriver = jdbcDriverMap.get(dbType);
    String datasourceName = config.getName();

    Properties p = new Properties();
    p.setProperty("url", url);
    p.setProperty("user", username);
    p.setProperty("password", password);

    AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
    ds.setUniqueResourceName(datasourceName);
    ds.setXaDataSourceClassName(jdbcDriver);
    ds.setXaProperties(p);
    return ds;
  }

  @Override
  public boolean isJTA() {
    return true;
  }

  @Override
  public UserTransaction createUserTransaction() {
    return new UserTransactionImp();
  }
}