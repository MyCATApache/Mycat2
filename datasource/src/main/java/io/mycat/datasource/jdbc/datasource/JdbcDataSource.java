package io.mycat.datasource.jdbc.datasource;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.plug.loadBalance.LoadBalanceElement;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class JdbcDataSource implements MycatDataSource, LoadBalanceElement {

  private final int index;
  private final DatasourceConfig datasourceConfig;
  private final JdbcReplica replica;
  private final boolean isMySQLType;

  public JdbcDataSource(int index, DatasourceConfig datasourceConfig,
      JdbcReplica replica) {
    this.index = index;
    this.datasourceConfig = datasourceConfig;
    this.replica = replica;
    String dbType = datasourceConfig.getDbType();
    this.isMySQLType = dbType == null || dbType.toUpperCase().contains("MYSQL");
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
//    AbsractJdbcConnectionManager sourceManager = new AbsractJdbcConnectionManager(SessionProviderImpl.INSYANCE,
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

  public abstract boolean isAlive();

  public String getName() {
    return datasourceConfig.getName();
  }

  @Override
  public boolean isMaster() {
    return this.replica.isMaster(this);
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

  public boolean isMySQLType() {
    return isMySQLType;
  }

  public String getDb() {
    return datasourceConfig.getInitDb();
  }

  public JdbcReplica getReplica() {
    return replica;
  }

  public abstract void heartBeat();

  public String getDbType() {
    return datasourceConfig.getDbType();
  }
}
