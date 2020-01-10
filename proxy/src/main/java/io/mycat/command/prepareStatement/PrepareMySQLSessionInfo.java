package io.mycat.command.prepareStatement;


import io.mycat.proxy.session.SessionManager.SessionIdAble;

/**
 * @author jamie12221
 *  date 2019-04-30 16:24
 *  预处理句柄与session绑定
 **/
public final class PrepareMySQLSessionInfo implements SessionIdAble {
  long statementId;
  int sessionId;

  public long getStatementId() {
    return statementId;
  }

  public int getSessionId() {
    return sessionId;
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

    if (statementId != item.statementId) {
      return false;
    }
    return sessionId == item.sessionId;
  }

  @Override
  public int hashCode() {
    int result = (int) (statementId ^ (statementId >>> 32));
    result = 31 * result + sessionId;
    return result;
  }

  public PrepareMySQLSessionInfo(long statementId, int sessionId) {
    this.statementId = statementId;
    this.sessionId = sessionId;
  }


}