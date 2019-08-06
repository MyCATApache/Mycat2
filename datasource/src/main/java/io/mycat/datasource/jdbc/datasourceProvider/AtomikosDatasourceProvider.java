package io.mycat.datasource.jdbc.datasourceProvider;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.mysql.cj.jdbc.MysqlXADataSource;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.JdbcDataSource;
import java.sql.SQLException;
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
    p.setProperty("com.atomikos.icatch.serial_jta_transactions", "false");
    AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
    ds.setXaProperties(p);
    ds.setConcurrentConnectionValidation(true);
    ds.setUniqueResourceName(datasourceName);
    ds.getXaProperties().setProperty("com.atomikos.icatch.serial_jta_transactions", "false");
    ds.setMaxPoolSize(65535);
    ds.setLocalTransactionMode(true);
    ds.setBorrowConnectionTimeout(60);
    ds.setReapTimeout(100000000);
    ds.setMaxLifetime(999999999);

    MysqlXADataSource mysqlXaDataSource = new MysqlXADataSource();
    mysqlXaDataSource.setURL(url);
    mysqlXaDataSource.setUser(username);
    mysqlXaDataSource.setPassword(password);
    try {
      mysqlXaDataSource.setPinGlobalTxToPhysicalConnection(true);
    } catch (SQLException e) {
      e.printStackTrace();
    }

    ds.setXaDataSource(mysqlXaDataSource);

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