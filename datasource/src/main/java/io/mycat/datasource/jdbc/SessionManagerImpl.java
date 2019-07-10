package io.mycat.datasource.jdbc;

import io.mycat.proxy.session.SessionManager.CheckResult;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import io.mycat.replica.MySQLDatasource;
import java.util.List;

public class SessionManagerImpl implements SessionManager {

  @Override
  public List<JdbcSession> getAllSessions() {
    return null;
  }

  @Override
  public int currentSessionCount() {
    return 0;
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

  @Override
  public JdbcSession getIdleSessionsOfKey(JdbcDataSource datasource) {
    return null;
  }

  @Override
  public void addIdleSession(JdbcSession session) {

  }

  @Override
  public void removeIdleSession(JdbcSession session) {

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
}