package io.mycat.proxy.reactor;

public interface NIOJob {

  void run(ReactorEnvThread reactor) throws Exception;

  void stop(ReactorEnvThread reactor, Exception reason);

  String message();
}