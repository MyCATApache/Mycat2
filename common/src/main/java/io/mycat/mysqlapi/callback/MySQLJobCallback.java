package io.mycat.mysqlapi.callback;

public interface MySQLJobCallback {

  void run() throws Exception;

  void stop(Exception reason);

  String message();
}