package io.mycat.compute;

import io.mycat.MycatException;

public interface PersistenceAction {

  RowBaseIterator createResultSet(QueryNavigator queryNavigator, Session session, Command command)
      throws MycatException;

  void update(Session session, Command command) throws MycatException;

  long begin(Session session) throws MycatException;

  void commit(Session session, long id) throws MycatException;

  void rollback(Session session, long id) throws MycatException;
}