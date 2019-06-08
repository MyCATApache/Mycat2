package io.mycat.rpc.publisher;

import io.mycat.rpc.RpcHandler;
import io.mycat.rpc.RpcSession;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

public class Publisher {


  private final ZContext context;
  private final Poller poller;
  private long timeout;
  private final ConcurrentLinkedQueue<BiConsumer<Poller, ZContext>> pending = new ConcurrentLinkedQueue<>();
  private final ArrayList<RpcSession> sessions = new ArrayList<>();

  public Publisher(long timeout, ZContext context) {
    this.timeout = timeout;
    this.context = context;
    poller = context.createPoller(0);
  }

  public void pending(BiConsumer<Poller, ZContext> task) {
    pending.add(task);
  }

  /**
   * Getter for property 'poller'.
   *
   * @return Value for property 'poller'.
   */
  Poller getPoller() {
    return poller;
  }

  void addSession(int index, RpcSession session) {
    sessions.add(index, session);
  }

  /**
   * Getter for property 'context'.
   *
   * @return Value for property 'context'.
   */
  ZContext getContext() {
    return context;
  }

  public void start() {
    Thread thread = Thread.currentThread();
    try {
      while (!thread.isInterrupted()) {
        if (!pending.isEmpty()) {
          Iterator<BiConsumer<Poller, ZContext>> iterator = pending.iterator();
          while (iterator.hasNext()) {
            iterator.next().accept(poller, context);
            iterator.remove();
          }
          continue;
        }
        int size = poller.getNext();
        int events = poller.poll(timeout);
        for (int i = 0; i < size; i++) {

          Socket socket = poller.getSocket(i);
          if (socket != null) {
            RpcSession rpcSession = sessions.get(i);
            RpcHandler rpcHandler = rpcSession.getRpcHandler();
            if (poller.pollin(i)) {
              rpcHandler.pollIn(rpcSession, socket, this);
            }
            if (poller.pollout(i)) {
              rpcHandler.pollOut(rpcSession, socket, this);
            }
            if (poller.pollerr(i)) {
              rpcHandler.pollErr(rpcSession, socket, this);
            }
          }

        }
      }
    } finally {
      context.destroy();
    }
  }
  public static void main(String[] args) {
    Publisher context = new Publisher(100,new ZContext());
    int publisher = context.createPublisher("tcp://localhost:5555", new RpcHandler() {
      @Override
      public void pollOut(RpcSession session, Socket socket, Publisher rpc) {
        boolean send = socket.send("123", ZMQ.NOBLOCK);
      }

      @Override
      public void pollErr(RpcSession session, Socket socket, Publisher rpc) {
        System.out.println(socket.errno());
      }
    });
    context.start();

  }


  public int createPublisher(String addr, RpcHandler handler) {
    PublisherImpl mycatPublisher = new PublisherImpl();
    return mycatPublisher.connect(addr, this, handler);
  }


  public int createReceiver(String addr, RpcHandler handler) {
    ReceiverImpl mycatReceiver = new ReceiverImpl();
    return mycatReceiver.connect(addr, this, handler);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    poller.close();
    Collections.unmodifiableCollection(context.getSockets()).forEach(i -> i.close());
    context.close();
  }

}