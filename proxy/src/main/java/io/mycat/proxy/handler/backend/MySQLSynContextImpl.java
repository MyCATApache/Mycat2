package io.mycat.proxy.handler.backend;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MySQLClientSession;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class MySQLSynContextImpl extends MySQLSynContext{

  String deafultDatabase;
  MySQLIsolation isolation;
  MySQLAutoCommit autoCommit;
  String charset;
  String characterSetResult;

  long sqlSelectLimit = -1;  //Statement: SET sqlSelectLimit=99
  long netWriteTimeout = -1;
  boolean readOnly;

//
//  public MySQLSynContextImpl(MySQLClientSession session) {
//    this( session.getDefaultDatabase(),
//        session.getIsolation(),
//        session.isAutomCommit(),
//        session.getCharset(),
//        session.getCharacterSetResult(), session.getSelectLimit(), session.getNetWriteTimeout(),
//        session.isReadOnly());
//
//  }


  public MySQLSynContextImpl(String deafultDatabase, MySQLIsolation isolation,
                             MySQLAutoCommit autoCommit, String charset, String characterSetResult,
                             long sqlSelectLimit,
                             long netWriteTimeout,
                             boolean readOnly) {
    this.deafultDatabase = deafultDatabase;
    this.isolation = isolation;
    this.autoCommit = autoCommit;
    this.charset = charset;
    this.characterSetResult = characterSetResult;
    this.sqlSelectLimit = sqlSelectLimit;
    this.netWriteTimeout = netWriteTimeout;
    this.readOnly = readOnly;
  }

  public void successSyncMySQLClientSession(MySQLClientSession mysql) {
    mysql.setCharset(charset);
    mysql.setDefaultDatabase(deafultDatabase);
    mysql.setIsolation(isolation);
    mysql.setCharacterSetResult(characterSetResult);
    mysql.setSelectLimit(sqlSelectLimit);
    mysql.setNetWriteTimeout(netWriteTimeout);
    if (autoCommit != mysql.isAutomCommit()) {
      throw new MycatException("sync autocommit " + mysql.sessionId() + " fail");
    }
    if (readOnly != mysql.isReadOnly()) {
      throw new MycatException("sync readonly" + mysql.sessionId() + " fail");
    }
  }

  public String getSyncSQL() {
    return "USE " + deafultDatabase + ";"
        + isolation.getCmd() + autoCommit.getCmd()
        + "SET names " + charset + ";"
        + ("SET character_set_results =" + (
        characterSetResult == null || "".equals(characterSetResult) ? "NULL"
            : characterSetResult)) + ";"
        + ("SET SQL_SELECT_LIMIT=" + ((sqlSelectLimit == -1) ? "DEFAULT" : sqlSelectLimit) + ";"
        + ("SET net_write_timeout=" + (netWriteTimeout == -1 ? "default" : netWriteTimeout)) + ";"
    );
  }

  @Override
  public void onSynchronizationStateLog(MySQLClientSession mysql) {
    MycatMonitor.onSynchronizationState(mysql);
  }

  @Override
  public String getDefaultDatabase() {
    return deafultDatabase;
  }

}