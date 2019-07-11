package io.mycat.datasource.jdbc;


import io.mycat.MycatException;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class JdbcDataSourceManager implements SessionManager {

  private final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(JdbcDataSourceManager.class);
  private final static Set<String> AVAILABLE_JDBC_DATA_SOURCE = new HashSet<>();
  private final ConcurrentHashMap<Integer, JdbcSession> allSessions = new ConcurrentHashMap<>(8192);
  private final HashMap<JdbcDataSource, DataSource> dataSourceMap = new HashMap<>();
  private final DatasourceProvider datasourceProvider;
  private final ProxyRuntime runtime;

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

  public JdbcDataSourceManager(ProxyRuntime runtime,
      DatasourceProvider provider, List<JdbcDataSource> dataSources) {
    Objects.requireNonNull(runtime);
    Objects.requireNonNull(provider);
    Objects.requireNonNull(dataSources);
    this.runtime = runtime;
    this.datasourceProvider = provider;

    for (JdbcDataSource dataSource : dataSources) {
      DataSource pool = datasourceProvider
          .createDataSource(dataSource.getUrl(), dataSource.getUsername(),
              dataSource.getPassword());
      dataSourceMap.put(dataSource, pool);
    }

  }

  public List<JdbcSession> getAllSessions() {
    return new ArrayList<>(allSessions.values());
  }

  @Override
  public int currentSessionCount() {
    return allSessions.size();
  }


  private DataSource getPool(JdbcDataSource datasource) {
    return dataSourceMap.get(datasource);
  }

  @Override
  public void clearAndDestroyDataSource(boolean normal, JdbcDataSource key, String reason) {
    for (Entry<Integer, JdbcSession> entry : allSessions.entrySet()) {
      JdbcSession session = entry.getValue();
      if (session.getDatasource().equals(key)) {
        session.close(normal, reason);
      }
    }
  }


  public JdbcSession createSession(JdbcDataSource key) throws MycatException {
    if (!key.isAlive()) {
      throw new MycatException("{} is not alive!", key.getName());
    }
    Connection connection = null;
    try {
      connection = getConnection(key);
    } catch (SQLException e) {
      throw new MycatException(e);
    }
    int sessionId = runtime.genSessionId();
    JdbcSession jdbcSession = new JdbcSession(sessionId, key);
    jdbcSession.wrap(connection);
    allSessions.put(sessionId, jdbcSession);
    return jdbcSession;
  }

  @Override
  public void closeSession(JdbcSession session, boolean normal, String reason) {
    try {
      session.connection.close();
    } catch (Exception e) {
      LOGGER.error("{}", e);
    }
    allSessions.remove(session.sessionId());
  }


  private Connection getConnection(JdbcDataSource key) throws SQLException {
    DataSource pool = getPool(key);
    return pool.getConnection();
  }

  interface DatasourceProvider {

    DataSource createDataSource(String url, String username, String password);
  }
}
