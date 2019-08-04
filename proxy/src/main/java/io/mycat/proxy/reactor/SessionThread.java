package io.mycat.proxy.reactor;

import io.mycat.proxy.session.Session;

public class SessionThread extends Thread {

  private Session session;

  public SessionThread() {
  }

  public SessionThread(Runnable target) {
    super(target);
  }

  public SessionThread(Runnable target, String name) {
    super(target, name);
  }

  public Session getCurSession() {
    return session;
  }

  public void setCurSession(Session session){
    this.session = session;
  }

  public void close() {

  }
}