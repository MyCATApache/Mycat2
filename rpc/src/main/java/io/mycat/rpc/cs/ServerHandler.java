package io.mycat.rpc.cs;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public interface ServerHandler {
  long getLastActiveTime();
  void updateActiveTime();
  void onRevc(ZMsg identity, int clientId, byte[] content, Socket worker);

  void close();
}