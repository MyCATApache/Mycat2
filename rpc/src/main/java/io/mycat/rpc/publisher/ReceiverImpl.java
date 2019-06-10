package io.mycat.rpc.publisher;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

class ReceiverImpl implements PublisherSession {

  private String addr;
  private Publisher context;
  private PublisherHandler handler;
  private ZMQ.Socket subscriber;
  private int id;
  private Socket socket;

  /**
   * @param addr tcp://localhost:5555
   */
  public int connect(String addr, Publisher rpc, PublisherHandler handler) {
    this.addr = addr;
    this.context = rpc;
    this.handler = handler;
    ZContext context = rpc.getContext();
    socket = context.createSocket(SocketType.SUB); //subscribe类型
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
  public PublisherHandler getHandler() {
    return handler;
  }

  @Override
  public Socket getSocket() {
    return socket;
  }

  public static void main(String[] args) {
    Publisher context = new Publisher(100,new ZContext());
    int receiver = context.createReceiver("tcp://localhost:5555", new PublisherHandler() {
      @Override
      public void pollIn(PublisherSession session, Socket socket, Publisher rpc) {
        String s = socket.recvStr();
        System.out.println(s);
      }

      @Override
      public void pollErr(PublisherSession session, Socket socket, Publisher rpc) {

      }
    });
    context.process(true);
  }
}