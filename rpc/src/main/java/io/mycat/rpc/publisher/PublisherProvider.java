package io.mycat.rpc.publisher;

import io.mycat.rpc.Handler;
import io.mycat.rpc.RpcSocket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Error;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

/**
 * The type Publisher provider.
 */
public class PublisherProvider {


  private final ZContext context;
  private final Poller poller;
  private long loopTime;
  private final ConcurrentLinkedQueue<BiConsumer<Poller, ZContext>> pending = new ConcurrentLinkedQueue<>();
  private final ArrayList<Handler> hanlders = new ArrayList<>();
  private PublishSocket wrapRpcSocket;

  /**
   * Instantiates a new Publisher provider.
   *
   * @param loopTime microseconds
   * @param context the context
   */
  public PublisherProvider(long loopTime, ZContext context) {
    this.loopTime = loopTime;
    this.context = context;
    poller = context.createPoller(0);
    wrapRpcSocket = new PublishSocket();
  }

  /**
   * Pending.
   *
   * @param task the task
   */
  public void pending(BiConsumer<Poller, ZContext> task) {
    pending.add(task);
  }

  /**
   * Loop.
   */
  public void loop() {
    Thread thread = Thread.currentThread();
    try {
      while (!thread.isInterrupted()) {
        process();
      }
    } finally {
      close();
    }
  }

  /**
   * Process boolean.
   *
   * @return the boolean
   */
  public boolean process() {
    int errno = 0;
    int events = poller.poll(loopTime);
    int size = poller.getNext();
    for (int i = 0; i < size; i++) {
      Socket socket = poller.getSocket(i);
      if (socket != null) {
        Handler hanlder = hanlders.get(i);
        if (poller.pollerr(i)) {
          Error byCode = Error.findByCode(errno);
          System.out.println(byCode);
          poller.unregister(socket);
          socket.setLinger(0);
          socket.close();
          wrapRpcSocket.setSocket(socket);
          hanlder.pollErr(wrapRpcSocket, this,byCode.getMessage());
          continue;
        }
        if (poller.pollin(i)) {
          ConsumerHandler p = (ConsumerHandler) hanlder;
          p.pollIn(socket.recv(), this);
        }
        if (hanlder instanceof PublisherHandler) {
          PublisherHandler p = (PublisherHandler) hanlder;
          wrapRpcSocket.setSocket(socket);
          p.pollOut(wrapRpcSocket, this);
        }
      }
    }
    if (!pending.isEmpty()) {
      Iterator<BiConsumer<Poller, ZContext>> iterator = pending.iterator();
      while (iterator.hasNext()) {
        iterator.next().accept(poller, context);
        iterator.remove();
      }
      return true;
    }
    return false;
  }

  /**
   * Close.
   */
  public void close() {
    int size = poller.getSize();
    for (int i = 0; i < size; i++) {
      Socket socket = poller.getSocket(i);
      if (socket != null) {
        socket.setLinger(0);
        socket.close();
      }
    }
    poller.close();
  }

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   */
  public static void main(String[] args) {
    PublisherProvider loop = new PublisherProvider(1, new ZContext());
    int publisher = loop.addPublisher("tcp://localhost:5555", new PublisherHandler() {
      @Override
      public void pollOut(RpcSocket socket, PublisherProvider rpc) {
        socket.send("123");
        System.out.println("send");
      }

      @Override
      public void pollErr(RpcSocket wrapRpcSocket, PublisherProvider publisherLoop,
          String message) {

      }
    },true);
    loop.addReceiver("tcp://localhost:5555",new byte[]{},new ConsumerHandler(){

      @Override
      public void pollErr(RpcSocket wrapRpcSocket, PublisherProvider publisherLoop,
          String message) {

      }

      @Override
      public void pollIn(byte[] bytes, PublisherProvider rpc) {
        System.out.println(new String(bytes));
      }
    },false);
    loop.loop();
  }

  private static RpcSocket createPublisher(ZContext context,String addr, boolean bind) {
    Socket socket = context.createSocket(SocketType.PUB); //subscribe类型
    if (bind){
      socket.bind(addr);
    }else {
      socket.connect(addr);
    }
    PublishSocket publishSocket = new PublishSocket();
    publishSocket.setSocket(socket);
    return publishSocket;
  }


  /**
   * Add publisher int.
   *
   * @param addr the addr
   * @param handler the handler
   * @param bind the bind
   * @return the int
   */
  public int addPublisher(String addr, PublisherHandler handler,boolean bind) {
    Objects.requireNonNull(addr);
    Objects.requireNonNull(handler);
    Socket socket = context.createSocket(SocketType.PUB); //subscribe类型
    if (bind){
      String[] strings = addr.split(",");
      for (String s : strings) {
        socket.bind(s);
      }
    }else {
      socket.connect(addr);
    }
    int register = poller.register(socket, Poller.POLLERR);
    hanlders.add(handler);
    return register;
  }


  /**
   * Add receiver int.
   *
   * @param addr the addr
   * @param topic the topic
   * @param handler the handler
   * @param bind the bind
   * @return the int
   */
  public int addReceiver(String addr, byte[] topic, ConsumerHandler handler,boolean bind) {
    Socket socket = context.createSocket(SocketType.SUB); //subscribe类型
    if (bind){
      socket.bind(addr);
    }else {
      socket.connect(addr);
    }
    int register = poller.register(socket, Poller.POLLIN | Poller.POLLERR);
    socket.subscribe(topic); //只订阅Time: 开头的信息
    hanlders.add(handler);
    return register;
  }

}