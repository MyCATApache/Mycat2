package io.mycat.jdbc;

import io.mycat.MycatExpection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author jamie12221
 * @date 2019-05-10 14:46
 **/
public class JdbcDataSourceManager {
  private final LinkedList<JdbcSession> allSessions = new LinkedList<>();
  private final HashMap<JdbcDataSource, LinkedList<JdbcSession>> idleDatasourcehMap = new HashMap<>();
  private int count = 0;
  public Collection<JdbcSession> getAllSessions() {
    return Collections.unmodifiableCollection(allSessions);
  }

  public int curSessionCount() {
    return count;
  }


  public void removeSession(JdbcSession session) {
    LinkedList<JdbcSession> mySQLSessions = idleDatasourcehMap.get(session.getDatasource());
    mySQLSessions.remove(session);
    if (allSessions.remove(session)) {
      count--;
      clearAndDestroyMySQLSession(session,"removeSession");
    }
  }

  public JdbcSession getIdleSessionsOfKey(JdbcDataSource datasource) throws SQLException {
    if (!datasource.isAlive()) {
      throw new MycatExpection(datasource.getName() + " is not alive!");
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
    mySQLSessions.remove(session);
  }

  public void clearAndDestroyMySQLSession(JdbcSession dsMetaBean, String reason) {

  }

  public JdbcSession createSession(JdbcDataSource key) throws MycatExpection {
    Connection connection = null;
    try {
      connection = DriverManager
                       .getConnection(key.getUrl(), key.getUsername(),
                           key.getPassword());
    } catch (SQLException e) {
      throw new MycatExpection(e.getLocalizedMessage());
    }
    JdbcSession jdbcSession = new JdbcSession(connection, key);
    count++;
    allSessions.add(jdbcSession);
    return jdbcSession;
  }
}
