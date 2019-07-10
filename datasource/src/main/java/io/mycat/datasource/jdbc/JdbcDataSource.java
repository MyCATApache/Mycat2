package io.mycat.datasource.jdbc;

import io.mycat.compute.RowBaseIterator;
import io.mycat.compute.RowMetaData;
import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiver;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jamie12221 date 2019-05-10 13:21
 **/
public class JdbcDataSource {

  private final int index;
  private final DatasourceConfig datasourceConfig;
  private volatile boolean isAlive = false;

  public JdbcDataSource(int index, DatasourceConfig datasourceConfig) {
    this.index = index;
    this.datasourceConfig = datasourceConfig;
  }


  public static void main(String[] args) throws SQLException, IOException {
    RowBaseIterator query = getSimple();

    RowMetaData rowMetaData = query.metaData();
    while (query.next()) {
      String string = query.getString(1);
      System.out.println(string);
    }

    System.out.println();
  }

  public static RowBaseIterator getSimple() throws IOException, SQLException {
    ConfigReceiver configReceiver = ConfigLoader
        .load("D:\\newgit\\f\\mycat2\\src\\main\\resources");
    ReplicasRootConfig config = configReceiver.getConfig(ConfigEnum.DATASOURCE);
    ReplicaConfig replicaConfig = config.getReplicas().get(0);
    List<JdbcDataSource> jdbcDataSources = initJdbcDatasource(replicaConfig);
    JdbcDataSource jdbcDataSource = jdbcDataSources.get(0);

    JdbcDataSourceManager sourceManager = new JdbcDataSourceManager(SessionProviderImpl.INSYANCE,
        DatasourceProviderImpl.INSTANCE);
    JdbcSession session = sourceManager.createSession(jdbcDataSource);
    return session.query("SELECT * FROM `information_schema`.`COLUMNS`;");
  }


  public static List<JdbcDataSource> initJdbcDatasource(ReplicaConfig replicaConfig)
      throws SQLException {
    List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
    List<JdbcDataSource> datasourceList = new ArrayList<>();
    for (int index = 0; index < mysqls.size(); index++) {
      DatasourceConfig datasourceConfig = mysqls.get(index);
      if (datasourceConfig.getDbType() != null) {
        datasourceList.add(new JdbcDataSource(index, datasourceConfig));
      }
    }
    return datasourceList;
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
