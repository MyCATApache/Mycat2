package io.mycat.logTip;

/**
 * @author jamie12221
 * @date 2019-05-11 21:53
 **/
public enum SessionTip {
  CANNOT_SWITCH_DATANODE("cannot switch dataNode  maybe session in transaction");
  String message;

  SessionTip(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }}
