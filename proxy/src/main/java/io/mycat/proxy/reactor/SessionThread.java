package io.mycat.proxy.reactor;

import io.mycat.proxy.session.Session;

public interface SessionThread {

  Session getCurSession();

  void setCurSession(Session session);
}