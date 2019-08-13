package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.MycatConfigUtil;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class JdbcDataSource implements MycatDataSource {

  private final int index;
  private final DatasourceConfig datasourceConfig;
  private final JdbcReplica replica;
  private final boolean isMySQLType;
  final AtomicInteger counter = new AtomicInteger(0);
  final PhysicsInstance instance;

  public JdbcDataSource(int index, DatasourceConfig datasourceConfig,
      JdbcReplica replica) {
    this.index = index;
    this.datasourceConfig = datasourceConfig;
    this.replica = replica;
    String dbType = datasourceConfig.getDbType();
    this.isMySQLType = MycatConfigUtil.isMySQLType(datasourceConfig);
    this.instance = ReplicaSelectorRuntime.INSTCANE
        .registerDatasource(replica.getName(), datasourceConfig,
        index, () -> counter.get());
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

  public String getName() {
    return datasourceConfig.getName();
  }

  public int getIndex() {
    return index;
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

  public int getMaxCon() {
    return datasourceConfig.getMaxCon();
  }

  public abstract void heartBeat();

  public String getDbType() {
    return datasourceConfig.getDbType();
  }

  public PhysicsInstance instance() {
    return instance;
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
}
