package io.mycat.datasource.jdbc;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;

public class JdbcSyncContext {

  MySQLIsolation isolation;
  MySQLAutoCommit autoCommit;
  String charset;

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
}