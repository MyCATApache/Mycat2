/**
 * Copyright (C) <2019>  <mycat>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import java.nio.charset.Charset;


/**
 * 集中处理mysql服务器状态
 *
 * @author jamie12221
 *  date 2019-05-10 13:21
 **/
public final class MySQLServerStatus {
  private String lastMessage;
  private int affectedRows;
  private int serverStatus;
  private int warningCount;
  private long lastInsertId;
  private int serverCapabilities;
  private int lastErrorCode;
  static final byte[] state = "HY000".getBytes();
  private byte[] SQL_STATE = state;
  private String charsetName;
  private Charset charset;
  private int charsetIndex;
  private MySQLAutoCommit autoCommit;
  private MySQLIsolation isolation = MySQLIsolation.REPEATED_READ;
  protected boolean localInFileRequestState = false;
  private long selectLimit = -1;
  private long netWriteTimeout = -1;
  private boolean accessModeReadOnly = false;

  public boolean multiStatementSupport = false;
  private String charsetSetResult;

  public boolean isMultiStatementSupport() {
    return multiStatementSupport;
  }

  public void setMultiStatementSupport(boolean multiStatementSupport) {
    this.multiStatementSupport = multiStatementSupport;
  }

  public boolean getLocalInFileRequestState() {
    return localInFileRequestState;
  }

  public void setLocalInFileRequestState(boolean localInFileRequestState) {
    this.localInFileRequestState = localInFileRequestState;
  }

  public long incrementAffectedRows() {
    return ++affectedRows;
  }

  public long incrementWarningCount() {
    return ++warningCount;
  }

  public String getCharsetName() {
    return charsetName;
  }

  public int getCharsetIndex() {
    return charsetIndex;
  }

  public Charset getCharset() {
    return charset;
  }

  public void setCharset(int charsetIndex, String charsetName, Charset charset) {
    this.charsetIndex = charsetIndex;
    this.charsetName = charsetName;
    this.charset = charset;
  }
  public MySQLAutoCommit getAutoCommit() {
    return autoCommit;
  }

  public void setAutoCommit(MySQLAutoCommit autoCommit) {
    this.autoCommit = autoCommit;
    switch (autoCommit) {
      case ON:
        this.serverStatus |= MySQLServerStatusFlags.AUTO_COMMIT;
        break;
      case OFF:
        this.serverStatus &= ~MySQLServerStatusFlags.AUTO_COMMIT;
        break;
    }
  }

  public MySQLIsolation getIsolation() {
    return isolation;
  }

  public void setIsolation(MySQLIsolation isolation) {
    this.isolation = isolation;
  }

  public String getLastMessage() {
    return lastMessage == null?"":lastMessage;
  }

  public void setLastMessage(String lastMessage) {
    this.lastMessage = lastMessage;
  }

  public int getAffectedRows() {
    return affectedRows;
  }

  public void setAffectedRows(int affectedRows) {
    this.affectedRows = affectedRows;
  }

  public int getServerStatus() {
    return serverStatus;
  }

  public int setServerStatus(int serverStatus) {
    return this.serverStatus = serverStatus;
  }

  public int getWarningCount() {
    return warningCount;
  }

  public void setWarningCount(int warningCount) {
    this.warningCount = warningCount;
  }

  public long getLastInsertId() {
    return lastInsertId;
  }

  public void setLastInsertId(long lastInsertId) {
    this.lastInsertId = lastInsertId;
  }

  public int getServerCapabilities() {
    return serverCapabilities;
  }

  public void setServerCapabilities(int serverCapabilities) {
    this.serverCapabilities = serverCapabilities;
  }

  public int getLastErrorCode() {
    return lastErrorCode;
  }

  public void setLastErrorCode(int lastErrorCode) {
    this.lastErrorCode = lastErrorCode;
  }

  public byte[] getSqlState() {
    return SQL_STATE == null ? state : SQL_STATE;
  }

  public String getCharsetSetResult() {
    return charsetSetResult;
  }

  public void setCharsetSetResult(String charsetSetResult) {
    this.charsetSetResult = charsetSetResult;
  }

  public long getSelectLimit() {
    return selectLimit;
  }

  public void setSelectLimit(long selectLimit) {
    this.selectLimit = selectLimit;
  }

  public void setNetWriteTimeout(long netWriteTimeout) {
    this.netWriteTimeout = netWriteTimeout;
  }

  public long getNetWriteTimeout() {
    return netWriteTimeout;
  }

  public boolean isAccessModeReadOnly() {
    return accessModeReadOnly;
  }

  public void setAccessModeReadOnly(boolean accessModeReadOnly) {
    this.accessModeReadOnly = accessModeReadOnly;
  }

}
