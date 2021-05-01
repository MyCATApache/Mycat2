/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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