package io.mycat.command.prepareStatement;

import io.mycat.replica.MySQLDatasource;

public class PrepareMySQLSessionInfo {

  MySQLDatasource datasource;
  long id;
  int sessionId;

  public long getId() {
    return id;
  }

  public int getSessionId() {
    return sessionId;
  }

  /**
   * Getter for property 'datasource'.
   *
   * @return Value for property 'datasource'.
   */
  public MySQLDatasource getDatasource() {
    return datasource;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PrepareMySQLSessionInfo item = (PrepareMySQLSessionInfo) o;

    if (id != item.id) {
      return false;
    }
    return sessionId == item.sessionId;
  }

  @Override
  public int hashCode() {
    int result = (int) (id ^ (id >>> 32));
    result = 31 * result + sessionId;
    return result;
  }


  public PrepareMySQLSessionInfo(MySQLDatasource datasource, long id, int sessionId) {
    this.datasource = datasource;
    this.id = id;
    this.sessionId = sessionId;
  }


}