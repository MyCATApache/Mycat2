package io.mycat.proxy.handler.backend;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.proxy.session.MySQLClientSession;

public abstract class MySQLSynContext {

  public abstract void successSyncMySQLClientSession(MySQLClientSession mysql);

  public abstract boolean equals(Object o);

  public abstract int hashCode();

  public abstract String getSyncSQL();

  public abstract void onSynchronizationStateLog(MySQLClientSession mysql);

  public abstract MySQLDataNode getDataNode();
}