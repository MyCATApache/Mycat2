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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author jamie12221
 *  date 2019-05-10 13:21
 **/
public class JdbcDataSource {
  private final int index;
  private final DatasourceConfig datasourceConfig;

  public JdbcDataSource(int index, DatasourceConfig datasourceConfig) throws SQLException {
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

    JdbcDataSourceManager sourceManager = new JdbcDataSourceManager();
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


  final static Set<String> AVAILABLE_JDBC_DATA_SOURCE = new HashSet<>();

  static {
    // 加载可能的驱动
    List<String> drivers = Arrays.asList(
        "com.mysql.jdbc.Driver");

    for (String driver : drivers) {
      try {
        Class.forName(driver);
        AVAILABLE_JDBC_DATA_SOURCE.add(driver);
      } catch (ClassNotFoundException ignored) {
      }
    }
  }

  public String getUrl() {
    return datasourceConfig.getUrl();
  }

  public String getUsername() {
    return datasourceConfig.getUser();
  }

  public String getPassword() {
    return  datasourceConfig.getPassword();
  }

  public boolean isAlive() {
    return true;
  }

  public String getName() {
    return datasourceConfig.getName();
  }
}
