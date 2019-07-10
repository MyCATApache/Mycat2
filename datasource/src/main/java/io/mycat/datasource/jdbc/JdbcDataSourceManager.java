package io.mycat.datasource.jdbc;

import static io.mycat.datasource.jdbc.JdbcDataSource.AVAILABLE_JDBC_DATA_SOURCE;

import io.mycat.MycatException;
import io.mycat.proxy.session.SessionManager.CheckResult;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import io.mycat.replica.MySQLDatasource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author jamie12221
 *  date 2019-05-10 14:46
 *  该类型需要并发处理
 **/
public class JdbcDataSourceManager implements SessionManager {
  private final LinkedList<JdbcSession> allSessions = new LinkedList<>();
  private final HashMap<JdbcDataSource, LinkedList<JdbcSession>> idleDatasourcehMap = new HashMap<>();

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

  public List<JdbcSession> getAllSessions() {
    return new ArrayList<>(allSessions);
  }

  @Override
  public int currentSessionCount() {
    return allSessions.size();
  }

  @Override
  public JdbcSession getIdleSessionsOfIdsOrPartial(JdbcDataSource datasource,
      List<SessionIdAble> ids, PartialType partialType) {
    return null;
  }

  @Override
  public JdbcSession getIdleMySQLClientSessionsByIds(JdbcDataSource datasource,
      List<SessionIdAble> ids, PartialType partialType) {
    return null;
  }

  public JdbcSession getIdleSessionsOfKey(JdbcDataSource datasource) {
    if (!datasource.isAlive()) {
      throw new MycatException(datasource.getName() + " is not alive!");
    } else {
      LinkedList<JdbcSession> mySQLSessions = this.idleDatasourcehMap.get(datasource);
      if (mySQLSessions == null || mySQLSessions.isEmpty()) {
        return createSession(datasource);
      } else {
     return
            ThreadLocalRandom.current().nextBoolean() ? mySQLSessions.removeFirst()
                : mySQLSessions.removeLast();
      }
    }
  }


  @Override
  public void clearAndDestroyDataSource(MySQLDatasource key, String reason) {

  }

  @Override
  public void idleConnectCheck() {

  }

  @Override
  public JdbcSession createSession(MySQLDatasource key) {
    return null;
  }

  @Override
  public void removeSession(JdbcSession session, boolean normal, String reason) {

  }


  @Override
  public CheckResult check(int sessionId) {
    return null;
  }


  public void addIdleSession(JdbcSession session) {
    idleDatasourcehMap.compute(session.getDatasource(), (k, l) -> {
      if (l == null) {
        l = new LinkedList<>();
      }
      l.add(session);
      return l;
    });
  }


  public void removeIdleSession(JdbcSession session) {
    LinkedList<JdbcSession> mySQLSessions = idleDatasourcehMap.get(session.getDatasource());
    if (mySQLSessions == null) {

    }
  }

  public void clearAndDestroyMySQLSession(JdbcSession dsMetaBean, String reason) {

  }

  public JdbcSession createSession(JdbcDataSource key) throws MycatException {
    Connection connection = null;
    try {
      String url = key.getUrl();
      String username = key.getUsername();
      String password = key.getPassword();
      connection = DriverManager
          .getConnection(url, username, password);
    } catch (SQLException e) {
      throw new MycatException(e.getLocalizedMessage());
    }
    JdbcSession jdbcSession = new JdbcSession(connection, key);
    allSessions.add(jdbcSession);
    return jdbcSession;
  }
}
