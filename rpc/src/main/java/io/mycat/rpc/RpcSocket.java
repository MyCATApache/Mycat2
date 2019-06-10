package io.mycat.rpc;

import org.zeromq.SocketType;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class RpcSocket  {

  private  ZMsg msg;
  private  Socket socket;

  public RpcSocket(ZMsg msg, Socket socket) {
    this.msg = msg;
    this.socket = socket;
  }

  public static RpcSocket wrap(ZMsg msg,Socket socket) {
    return new RpcSocket(msg,socket);
  }

  public void send(byte[] data) {
    msg.addLast(data);
    msg.send(socket,true);
  }
}