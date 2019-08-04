package io.mycat.proxy.reactor;

import io.mycat.CloseableObject;
import io.mycat.proxy.session.Session;
import java.util.ArrayList;
import java.util.List;

public class SessionThread extends Thread implements CloseableObject {

  private Session session;
  private List<CloseableObject> closeableObjects = new ArrayList<>();

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

  public void setCurSession(Session session) {
    this.session = session;
  }

  public void close() {
    for (CloseableObject closeableObject : closeableObjects) {
      closeableObject.close();
    }
    closeableObjects.clear();
  }

  @Override
  public void onExceptionClose() {
    for (CloseableObject closeableObject : closeableObjects) {
      closeableObject.onExceptionClose();
    }
    closeableObjects.clear();
  }

}