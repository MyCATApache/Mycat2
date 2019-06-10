package io.mycat.rpc.cs;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class ServerHandlerImpl implements ServerHandler {
  ServerSessionHandler[] handlers = new ServerSessionHandler[256];
  long lastActiveTime;
  @Override
  public long getLastActiveTime() {
    return lastActiveTime;
  }

  @Override
  public void updateActiveTime() {
    lastActiveTime = System.currentTimeMillis();
  }

  @Override
  public void onRevc(ZMsg header, int clientId, byte[] content, Socket worker) {
    ServerSessionHandler handler = handlers[clientId];
    if (handler == null){
      handler = new ServerSessionHandlerImpl();
    }
    handler.revc(content,header,worker);
  }

  @Override
  public void close() {
    for (ServerSessionHandler handler : handlers) {
      if (handler!=null){
        handler.clear();
      }
    }

  }
}