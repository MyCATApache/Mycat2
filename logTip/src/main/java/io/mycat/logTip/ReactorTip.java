package io.mycat.logTip;

import java.util.Objects;

/**
 * @author jamie12221
 * @date 2019-05-13 14:45
 **/
public enum ReactorTip {
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

  public String getMessage(Object... args) {
    for (int i = 0; i < args.length; i++) {
      args[i] = Objects.toString(args[i]);
    }
    return String.format(this.getMessage(), args);
  }
}
