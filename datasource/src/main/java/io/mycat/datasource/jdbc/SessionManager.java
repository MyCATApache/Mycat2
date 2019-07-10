package io.mycat.datasource.jdbc;

import java.util.List;

public interface SessionManager {

  List<JdbcSession> getAllSessions();

  int currentSessionCount();


  void clearAndDestroyDataSource(boolean normal, JdbcDataSource key, String reason);

  JdbcSession createSession(JdbcDataSource key);

  void closeSession(JdbcSession session, boolean normal, String reason);

}