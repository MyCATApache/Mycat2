package io.mycat.datasource.jdbc;

import io.mycat.proxy.session.SessionManager.CheckResult;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import io.mycat.replica.MySQLDatasource;
import java.util.List;

public interface SessionManager {

  List<JdbcSession> getAllSessions();

  int currentSessionCount();

  JdbcSession getIdleSessionsOfIdsOrPartial(JdbcDataSource datasource, List<SessionIdAble> ids,
      PartialType partialType);

  JdbcSession getIdleMySQLClientSessionsByIds(JdbcDataSource datasource,
      List<SessionIdAble> ids, PartialType partialType);

  JdbcSession getIdleSessionsOfKey(JdbcDataSource datasource);

  void addIdleSession(JdbcSession session);

  void removeIdleSession(JdbcSession session);

  void clearAndDestroyDataSource(MySQLDatasource key, String reason);

  void idleConnectCheck();

  JdbcSession createSession(MySQLDatasource key);

  void removeSession(JdbcSession session, boolean normal, String reason);

  CheckResult check(int sessionId);

}