package io.mycat.logTip;

/**
 * @author jamie12221
 * @date 2019-05-13 14:45
 * 本类管理所有Reactor相关的日志或者异常提示
 **/
public enum ReactorTip implements LogTip {
  REGISTER_NEW_CONNECTION("Register new connection error %s"),
  PROCESS_NIO_JOB_EEROR("Run nio job err:  %s"),
  PROCESS_NIO_IO_EEROR("Socket IO err :  %s"),
  PROCESS_NIO_UNKNOWN_EEROR("Unknown err :  %s"),
  CLIENT_CONNECTED("New Client connected:   %s"),
  CONNECT_ERROR("Connect failed  %s  reason: %s"),
  ;
  String message;

  ReactorTip(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
