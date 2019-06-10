package io.mycat.rpc.cs;

import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class ServerSessionHandlerImpl implements ServerSessionHandler {

  private ServerHandler serverHandler;

  @Override
  public void setServerHandler(ServerHandler serverHandler) {
    this.serverHandler = serverHandler;
  }

  @Override
  public void revc(byte[] data, ZMsg msg, Socket worker) {
    msg.add(data);
    msg.send(worker);
  }

  @Override
  public void clear() {
  }
}