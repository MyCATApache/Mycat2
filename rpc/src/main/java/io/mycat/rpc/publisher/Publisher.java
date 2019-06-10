package io.mycat.rpc.publisher;

import java.util.ArrayList;
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
  private final ArrayList<PublisherSession> sessions = new ArrayList<>();

  /**
   * @param timeout microseconds
   */
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

  void addSession(int index, PublisherSession session) {
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

  public void process(boolean loop) {
    Thread thread = Thread.currentThread();
    int errno;
    try {
      while (!thread.isInterrupted()) {
        int size = poller.getNext();
        int events = poller.poll(timeout);
        for (int i = 0; i < size; i++) {

          Socket socket = poller.getSocket(i);
          if (socket != null) {
            PublisherSession rpcSession = sessions.get(i);
            PublisherHandler rpcHandler = rpcSession.getHandler();
            if (poller.pollin(i)) {
              rpcHandler.pollIn(rpcSession, socket, this);
            }
            if (poller.pollout(i)) {
              rpcHandler.pollOut(rpcSession, socket, this);
            }
            if (poller.pollerr(i) || (errno = socket.errno()) != 0) {
              rpcHandler.pollErr(rpcSession, socket, this);
              continue;
            }
          }
        }
        if (!pending.isEmpty()) {
          Iterator<BiConsumer<Poller, ZContext>> iterator = pending.iterator();
          while (iterator.hasNext()) {
            iterator.next().accept(poller, context);
            iterator.remove();
          }
          continue;
        }
        if (!loop) {
          break;
        }
      }
    } finally {

    }
  }

  public static void main(String[] args) {
    Publisher context = new Publisher(1000000, new ZContext());
    int publisher = context.createPublisher("tcp://localhost:5555", new PublisherHandler() {
      @Override
      public void pollOut(PublisherSession session, Socket socket, Publisher rpc) {
        boolean send = socket.send("123", ZMQ.NOBLOCK);
        ZMQ.sleep(1000);
      }

      @Override
      public void pollErr(PublisherSession session, Socket socket, Publisher rpc) {
        System.out.println(socket.errno());
      }
    });
    context.process(true);

  }


  public int createPublisher(String addr, PublisherHandler handler) {
    PublisherImpl mycatPublisher = new PublisherImpl();
    return mycatPublisher.connect(addr, this, handler);
  }


  public int createReceiver(String addr, PublisherHandler handler) {
    ReceiverImpl mycatReceiver = new ReceiverImpl();
    return mycatReceiver.connect(addr, this, handler);
  }

  @Override
  public void finalize() throws Throwable {
    super.finalize();
    poller.close();
    for (PublisherSession session : sessions) {
      session.getSocket().close();
    }
  }

}