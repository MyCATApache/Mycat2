package io.mycat.rpc.publisher;

import java.util.Objects;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

 class PublisherImpl implements PublisherSession {

  private int id;
  private String addr;
  private Publisher rpc;
  private Socket publisher;
  private PublisherHandler handler;
  private Socket socket;

  /**
   * @param addr "tcp://*:5555"
   */
  public int connect(String addr, Publisher rpc, PublisherHandler handler) {
    this.addr = addr;
    this.rpc = rpc;
    this.handler = handler;
    ZContext context = this.rpc.getContext();
    socket = context.createSocket(SocketType.PUB);//publish类型
    socket.bind(addr);
    id = rpc.getPoller().register(socket, Poller.POLLOUT | Poller.POLLERR);
    publisher = socket;
    rpc.addSession(id, this);
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
  public PublisherHandler getHandler() {
    return handler;
  }

  @Override
  public Socket getSocket() {
    return socket;
  }
}