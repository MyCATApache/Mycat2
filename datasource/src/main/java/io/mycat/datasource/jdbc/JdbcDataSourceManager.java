package io.mycat.datasource.jdbc;


import io.mycat.MycatException;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class JdbcDataSourceManager implements SessionManager {

  private final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(JdbcDataSourceManager.class);
  private final ConcurrentHashMap<Integer, JdbcSession> allSessions = new ConcurrentHashMap<>(8192);
  private final HashMap<JdbcDataSource, DataSource> dataSourceMap = new HashMap<>();
  private final DatasourceProvider datasourceProvider;
  private final List<JdbcDataSource> dataSources;
  private final ReplicaDatasourceSelector<JdbcDataSource> selector;


  public JdbcDataSourceManager(GridRuntime runtime,
      DatasourceProvider provider, Map<String, String> jdbcDriverMap,
      List<JdbcDataSource> dataSources, ReplicaDatasourceSelector<JdbcDataSource> replica) {
    this.selector = replica;
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

  public JdbcSession createSessionDirectly(JdbcDataSource key) throws MycatException {
    Connection connection = null;
    try {
      connection = getConnection(key);
    } catch (SQLException e) {
      throw new MycatException(e);
    }
    JdbcSession jdbcSession = new JdbcSession((int) Thread.currentThread().getId(), key);
    SessionThread thread = (SessionThread) Thread.currentThread();
    thread.addCloseableObject(jdbcSession);
    jdbcSession.wrap(connection, this);
    allSessions.put(jdbcSession.sessionId(), jdbcSession);
    return jdbcSession;
  }

  public JdbcSession createSession(JdbcDataSource key) throws MycatException {
    return createSessionDirectly(key);
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

  public List<JdbcDataSource> getDatasourceList() {
    return dataSources;
  }

}
