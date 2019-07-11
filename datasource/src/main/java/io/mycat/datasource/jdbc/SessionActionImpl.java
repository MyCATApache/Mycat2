package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.compute.Command;
import io.mycat.compute.PersistenceAction;
import io.mycat.compute.QueryNavigator;
import io.mycat.compute.RowBaseIterator;
import io.mycat.compute.Session;
import java.util.Map;

public class SessionActionImpl implements PersistenceAction {

  private Map<String, JdbcDataNode> manager;

  public SessionActionImpl(
      Map<String, JdbcDataNode> manager) {
    this.manager = manager;
  }


  @Override
  public RowBaseIterator createResultSet(QueryNavigator queryNavigator, Session session,
      Command command) throws MycatException {
    String dataNodeName = command.getDataNodeName();
    JdbcDataNode jdbcDataNode = manager.get(dataNodeName);
    ReplicaDatasourceSelector replica = jdbcDataNode.getReplica();
    JdbcSession jdbcSession = replica.getJdbcSessionByBalance();
    return jdbcSession.query(command.querySQL());
  }

  @Override
  public void update(Session session, Command command) throws MycatException {

  }

  @Override
  public long begin(Session session) throws MycatException {
    return 0;
  }

  @Override
  public void commit(Session session, long id) throws MycatException {

  }

  @Override
  public void rollback(Session session, long id) throws MycatException {

  }
}