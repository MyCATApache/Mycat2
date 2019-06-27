package io.mycat.proxy.reactor;

public abstract class ReactorEnvThread extends Thread{
  protected final ReactorEnv reactorEnv = new ReactorEnv();

  public ReactorEnv getReactorEnv() {
    return reactorEnv;
  }

  public ReactorEnvThread() {
  }

  public ReactorEnvThread(Runnable target) {
    super(target);
  }

  public ReactorEnvThread(Runnable target, String name) {
    super(target, name);
  }
}