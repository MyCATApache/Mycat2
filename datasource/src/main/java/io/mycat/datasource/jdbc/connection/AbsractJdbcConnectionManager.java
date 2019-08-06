package io.mycat.datasource.jdbc.connection;


import io.mycat.MycatException;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class AbsractJdbcConnectionManager implements ConnectionManager {

  private final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(AbsractJdbcConnectionManager.class);
  private final HashMap<JdbcDataSource, DataSource> dataSourceMap = new HashMap<>();
  private final Set<Connection> unused = Collections.synchronizedSet(new HashSet<>());
  private final DatasourceProvider datasourceProvider;
  private final List<JdbcDataSource> dataSources;


  public AbsractJdbcConnectionManager(GridRuntime runtime,
      DatasourceProvider provider, Map<String, String> jdbcDriverMap,
      List<JdbcDataSource> dataSources) {
    Objects.requireNonNull(jdbcDriverMap);
    Objects.requireNonNull(runtime);
    Objects.requireNonNull(provider);
    Objects.requireNonNull(dataSources);
    this.datasourceProvider = provider;
    for (JdbcDataSource dataSource : dataSources) {
      DataSource pool = datasourceProvider
          .createDataSource(dataSource, jdbcDriverMap);
      dataSourceMap.put(dataSource, pool);
    }
    this.dataSources = dataSources;
  }

  private DataSource getPool(JdbcDataSource datasource) {
    return dataSourceMap.get(datasource);
  }


  public synchronized Connection getConnection(JdbcDataSource key) {
    DataSource pool = getPool(key);
    try {
      Connection connection = pool.getConnection();
      if (connection.isClosed()) {
        throw new MycatException("");
      }
//      if(!unused.isEmpty()&&!unused.contains(connection)){
//        closeConnection(connection);
//        return getConnection(key);
//      }
      return connection;
    } catch (SQLException e) {
      throw new MycatException(e);
    }
  }

  @Override
  public synchronized void closeConnection(Connection connection) {
    try {

      if (!isJTA()) {
        if (!connection.isClosed()) {
          connection.close();
        }
      } else {
        //gc
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public List<JdbcDataSource> getDatasourceList() {
    return dataSources;
  }

  public boolean isJTA() {
    return datasourceProvider.isJTA();
  }
}
