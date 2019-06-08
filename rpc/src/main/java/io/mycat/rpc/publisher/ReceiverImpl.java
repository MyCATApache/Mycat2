package io.mycat.rpc.publisher;

import io.mycat.rpc.RpcHandler;
import io.mycat.rpc.RpcSession;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

class ReceiverImpl implements RpcSession {

  private String addr;
  private Publisher context;
  private RpcHandler handler;
  private ZMQ.Socket subscriber;
  private int id;

  /**
   * @param addr tcp://localhost:5555
   */
  public int connect(String addr, Publisher rpc, RpcHandler handler) {
    this.addr = addr;
    this.context = rpc;
    this.handler = handler;
    ZContext context = rpc.getContext();
    ZMQ.Socket socket = context.createSocket(SocketType.SUB); //subscribe类型
    socket.connect(addr);
    id  = rpc.getPoller().register(socket, Poller.POLLIN | Poller.POLLERR);
    subscriber = socket;
    subscriber.subscribe(new byte[]{}); //只订阅Time: 开头的信息
    rpc.addSession(id,this);
    return id;
  }

  @Override
  public int id() {
    return id;
  }

  @Override
  public RpcHandler getRpcHandler() {
    return handler;
  }
}