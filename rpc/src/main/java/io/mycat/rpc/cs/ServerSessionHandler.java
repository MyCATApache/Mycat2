package io.mycat.rpc.cs;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public  interface ServerSessionHandler {
  void setServerHandler(ServerHandler serverHandler);
  void revc(byte[] data, ZMsg msg, Socket worker);

  void clear();
}