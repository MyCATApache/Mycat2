package io.mycat.logTip;

import io.mycat.beans.mysql.MySQLErrorCode;

/**
 * @author jamie12221
 * @date 2019-05-19 12:00
 **/
public enum SecurityTip implements LogTip {
  ACCESS_DENIED_ACCESS_DENIED(
      MySQLErrorCode.ER_ACCESS_DENIED_ERROR,
      "Access denied for user '" + "%s" + "' to database '" + "%s" + "'");

  String message;
  int errorCode;

  SecurityTip(int errorCode, String message) {
    this.errorCode = errorCode;
    this.message = message;
  }

  @Override
  public String getMessage() {
    return message;
  }}
