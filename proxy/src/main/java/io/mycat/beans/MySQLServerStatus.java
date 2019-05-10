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
 * chen junwen
 */
public class MySQLServerStatus {
  private String lastMessage;
  private long affectedRows;
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
  private String clientUser;
  private MySQLAutoCommit autoCommit;
  private MySQLIsolation isolation = MySQLIsolation.REPEATED_READ;

  public long incrementAffectedRows() {
    return ++affectedRows;
  }

  public int incrementWarningCount() {
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

  public String getClientUser() {
    return clientUser;
  }

  public void setClientUser(String clientUser) {
    this.clientUser = clientUser;
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

  public long getAffectedRows() {
    return affectedRows;
  }

  public void setAffectedRows(long affectedRows) {
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
}
