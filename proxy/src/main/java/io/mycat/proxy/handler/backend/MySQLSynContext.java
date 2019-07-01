package io.mycat.proxy.handler.backend;

import io.mycat.MycatException;
import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.util.Objects;

public class MySQLSynContext {

  MySQLDataNode dataNode;
  MySQLIsolation isolation;
  MySQLAutoCommit autoCommit;
  String charset;
  String characterSetResult;
  //Statement: SET sqlSelectLimit=99
  long sqlSelectLimit = -1;
  long netWriteTimeout = -1;
  boolean readOnly;
  ProxyRuntime runtime;


  public MySQLSynContext(MycatSession session) {
    this.runtime = session.getRuntime();
    Objects.requireNonNull(runtime);
    this.isolation = session.getIsolation();
    this.autoCommit = session.getAutoCommit();
    this.charset = session.getCharsetName();
    this.characterSetResult = session.getCharacterSetResults();
    this.dataNode = runtime.getDataNodeByName(session.getDataNode());
    this.sqlSelectLimit = session.getSelectLimit();
    this.netWriteTimeout = session.getNetWriteTimeout();
    this.readOnly = session.isAccessModeReadOnly();
  }

  public MySQLSynContext(MySQLClientSession session) {
    this((MySQLDataNode) session.getDataNode(),
        session.getIsolation(),
        session.isAutomCommit(),
        session.getCharset(),
        session.getCharacterSetResult(), session.getSelectLimit(), session.getNetWriteTimeout(),
        session.isReadOnly(),
        session.getRuntime());

  }


  public MySQLSynContext(MySQLDataNode dataNode, MySQLIsolation isolation,
      MySQLAutoCommit autoCommit, String charset, String characterSetResult,
      long sqlSelectLimit,
      long netWriteTimeout,
      boolean readOnly,
      ProxyRuntime runtime) {
    this.dataNode = dataNode;
    this.isolation = isolation;
    this.autoCommit = autoCommit;
    this.charset = charset;
    this.characterSetResult = characterSetResult;
    this.sqlSelectLimit = sqlSelectLimit;
    this.netWriteTimeout = netWriteTimeout;
    this.readOnly = readOnly;
    this.runtime = runtime;
    Objects.requireNonNull(runtime);
  }

  public void successSyncMySQLClientSession(MySQLClientSession mysql) {
    mysql.setCharset(charset);
    mysql.setDataNode(dataNode);
    mysql.setIsolation(isolation);
    mysql.setCharacterSetResult(characterSetResult);
    mysql.setSelectLimit(sqlSelectLimit);
    mysql.setNetWriteTimeout(netWriteTimeout);
    mysql.setReadOnly(readOnly);
    if (autoCommit != mysql.isAutomCommit()) {
      throw new MycatException("sync " + mysql.sessionId() + " fail");
    }
  }

  public MySQLDataNode getDataNode() {
    return dataNode;
  }


  public void setDataNode(MySQLDataNode dataNode) {
    this.dataNode = dataNode;
  }


  public MySQLIsolation getIsolation() {
    return isolation;
  }


  public void setIsolation(MySQLIsolation isolation) {
    this.isolation = isolation;
  }


  public MySQLAutoCommit getAutoCommit() {
    return autoCommit;
  }


  public void setAutoCommit(MySQLAutoCommit autoCommit) {
    this.autoCommit = autoCommit;
  }


  public String getCharset() {
    return charset;
  }


  public void setCharset(String charset) {
    this.charset = charset;
  }


  public String getCharacterSetResult() {
    return characterSetResult;
  }

  public void setCharacterSetResult(String characterSetResult) {
    this.characterSetResult = characterSetResult;
  }

  public long getSqlSelectLimit() {
    return sqlSelectLimit;
  }

  public long getNetWriteTimeout() {
    return netWriteTimeout;
  }

  public void setNetWriteTimeout(long netWriteTimeout) {
    this.netWriteTimeout = netWriteTimeout;
  }

  public String getSyncSQL() {
    return isolation.getCmd() + autoCommit.getCmd() + "USE " + dataNode.getSchemaName()
        + ";" + "SET names " + charset + ";"
        + ("SET character_set_results =" + (
        characterSetResult == null || "".equals(characterSetResult) ? "NULL"
            : characterSetResult)) + ";"
        + ("SET SQL_SELECT_LIMIT=" + ((sqlSelectLimit == -1) ? "DEFAULT" : sqlSelectLimit) + ";"
        + ("SET net_write_timeout=" + (netWriteTimeout == -1 ? "default" : netWriteTimeout)) + ";" +
        "set session transaction " + (readOnly ? "read only" : "read write"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MySQLSynContext that = (MySQLSynContext) o;

    if (sqlSelectLimit != that.sqlSelectLimit) {
      return false;
    }
    if (netWriteTimeout != that.netWriteTimeout) {
      return false;
    }
    if (readOnly != that.readOnly) {
      return false;
    }
    if (dataNode != null ? !dataNode.equals(that.dataNode) : that.dataNode != null) {
      return false;
    }
    if (isolation != that.isolation) {
      return false;
    }
    if (autoCommit != that.autoCommit) {
      return false;
    }
    if (charset != null ? !charset.equals(that.charset) : that.charset != null) {
      return false;
    }
    if (characterSetResult != null ? !characterSetResult.equals(that.characterSetResult)
        : that.characterSetResult != null) {
      return false;
    }
    return runtime != null ? runtime.equals(that.runtime) : that.runtime == null;
  }

  @Override
  public int hashCode() {
    int result = dataNode != null ? dataNode.hashCode() : 0;
    result = 31 * result + (isolation != null ? isolation.hashCode() : 0);
    result = 31 * result + (autoCommit != null ? autoCommit.hashCode() : 0);
    result = 31 * result + (charset != null ? charset.hashCode() : 0);
    result = 31 * result + (characterSetResult != null ? characterSetResult.hashCode() : 0);
    result = 31 * result + (int) (sqlSelectLimit ^ (sqlSelectLimit >>> 32));
    result = 31 * result + (int) (netWriteTimeout ^ (netWriteTimeout >>> 32));
    result = 31 * result + (readOnly ? 1 : 0);
    result = 31 * result + (runtime != null ? runtime.hashCode() : 0);
    return result;
  }
}