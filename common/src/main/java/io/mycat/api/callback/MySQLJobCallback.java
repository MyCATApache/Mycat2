package io.mycat.api.callback;

public interface MySQLJobCallback {

  void run() throws Exception;

  void stop(Exception reason);

  String message();
}