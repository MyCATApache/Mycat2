package io.mycat.datasource.jdbc;

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.compute.RowBaseIterator;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.plug.loadBalance.LoadBalanceElement;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @author jamie12221 date 2019-05-10 13:21
 **/
public class JdbcDataSource implements MycatDataSource, LoadBalanceElement {

  private final int index;
  private final DatasourceConfig datasourceConfig;
  private volatile boolean isAlive = true;

  public JdbcDataSource(int index, DatasourceConfig datasourceConfig) {
    this.index = index;
    this.datasourceConfig = datasourceConfig;
  }


  public static void main(String[] args) throws SQLException, IOException {
    RowBaseIterator query = getSimple();

    MycatRowMetaData rowMetaData = query.metaData();
    while (query.next()) {
      String string = query.getString(1);
      System.out.println(string);
    }

    System.out.println();
  }

  public static RowBaseIterator getSimple() throws IOException, SQLException {
//    ConfigReceiver configReceiver = ConfigLoader
//        .load("D:\\newgit\\f\\mycat2\\src\\main\\resources");
//    ReplicasRootConfig config = configReceiver.getConfig(ConfigEnum.DATASOURCE);
//    ReplicaConfig replicaConfig = config.getReplicas().get(0);
//    List<JdbcDataSource> jdbcDataSources = initJdbcDatasource(replicaConfig);
//
//
//    JdbcDataSourceManager sourceManager = new JdbcDataSourceManager(SessionProviderImpl.INSYANCE,
//        DruidDatasourceProvider.INSTANCE,jdbcDataSources);
//
//    JdbcDataSource jdbcDataSource = jdbcDataSources.get(0);
//    JdbcSession session = sourceManager.createSession(jdbcDataSource);
//    return session.query("SELECT * FROM `information_schema`.`COLUMNS`;");
    return null;
  }



  public String getUrl() {
    return datasourceConfig.getUrl();
  }

  public String getUsername() {
    return datasourceConfig.getUser();
  }

  public String getPassword() {
    return datasourceConfig.getPassword();
  }

  public boolean isAlive() {
    return isAlive;
  }

  public String getName() {
    return datasourceConfig.getName();
  }

  @Override
  public boolean isMaster() {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JdbcDataSource that = (JdbcDataSource) o;

    if (index != that.index) {
      return false;
    }
    return datasourceConfig != null ? datasourceConfig.equals(that.datasourceConfig)
        : that.datasourceConfig == null;
  }

  @Override
  public int hashCode() {
    int result = index;
    result = 31 * result + (datasourceConfig != null ? datasourceConfig.hashCode() : 0);
    return result;
  }

  public int getIndex() {
    return index;
  }

  public boolean asSelectRead() {
    return false;
  }

  @Override
  public int getSessionCounter() {
    return 0;
  }

  @Override
  public int getWeight() {
    return 0;
  }

  public String getDbType() {
    return  datasourceConfig.getDbType();
  }

  public String getDb() {
    return  datasourceConfig.getDb();
  }
}
