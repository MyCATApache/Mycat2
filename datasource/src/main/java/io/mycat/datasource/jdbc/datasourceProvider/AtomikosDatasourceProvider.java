package io.mycat.datasource.jdbc.datasourceProvider;

import com.alibaba.druid.pool.xa.DruidXADataSource;
import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.JdbcDataSource;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
    ds.setConcurrentConnectionValidation(false);
    ds.setUniqueResourceName(datasourceName);
    ds.setPoolSize(1);
    ds.setMaxPoolSize(65535);
    ds.setLocalTransactionMode(true);
    ds.setBorrowConnectionTimeout(60);
    ds.setReapTimeout(100000000);
    ds.setMaxLifetime(999999999);
//
//    MysqlXADataSource mysqlXaDataSource = new MysqlXADataSource();
//    mysqlXaDataSource.setURL(url);
//    mysqlXaDataSource.setUser(username);
//    mysqlXaDataSource.setPassword(password);

    DruidXADataSource datasource = new DruidXADataSource();
    datasource.setPassword(password);
    datasource.setUsername(username);
    datasource.setUrl(url);
    datasource.setMaxActive(100000);
    datasource.setMaxWait(TimeUnit.SECONDS.toMillis(5));
    try {
//      mysqlXaDataSource.setConnectTimeout(10000);
//      mysqlXaDataSource.setAutoReconnectForPools(true);
//      mysqlXaDataSource.setPinGlobalTxToPhysicalConnection(true);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // ds.setXaDataSource(mysqlXaDataSource);
    ds.setXaDataSource(datasource);
    return datasource;
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