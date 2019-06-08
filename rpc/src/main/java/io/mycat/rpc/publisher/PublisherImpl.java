package io.mycat.rpc.publisher;

import io.mycat.rpc.RpcHandler;
import io.mycat.rpc.RpcSession;
import java.util.Objects;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

class PublisherImpl implements RpcSession {

  private int id;
  private String addr;
  private Publisher rpc;
  private Socket publisher;
  private RpcHandler handler;
  /**
   * @param addr "tcp://*:5555"
   */
  public int connect(String addr, Publisher rpc,RpcHandler handler) {
    this.addr = addr;
    this.rpc = rpc;
    this.handler = handler;
    ZContext context = this.rpc.getContext();
    Socket socket = context.createSocket(SocketType.PUB);//publish类型
    socket.bind(addr);
    id = rpc.getPoller().register(socket, Poller.POLLOUT | Poller.POLLERR);
    publisher = socket;
    rpc.addSession(id,this);
    return id;
  }

  public void send(byte[] bytes) {
    Objects.requireNonNull(publisher);
    publisher.send(bytes, ZMQ.NOBLOCK);
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