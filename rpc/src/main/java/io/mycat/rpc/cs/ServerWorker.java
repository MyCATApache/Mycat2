package io.mycat.rpc.cs;
//Each worker task works on one request at a time and sends a random number
//of replies back, with random delays between replies:

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class ServerWorker {

  private ZContext ctx;
  private String addr;
  private boolean bind;
  private long timeout;
  private HashMap<Short, ServerHandler> handlerMap = new HashMap<>();
  private long lastActiveTime;

  /**
   * @param addr "inproc://backend"
   */
  public ServerWorker(ZContext ctx, String addr, boolean bind, long timeout) {
    this.ctx = ctx;
    this.addr = addr;
    this.bind = bind;
    this.timeout = timeout;
  }

  public void process(long pollTime) {
    Socket worker = ctx.createSocket(SocketType.DEALER);
    if (bind) {
      worker.bind(addr);
    } else {
      worker.connect(addr);
    }
    Poller poller = ctx.createPoller(1);
    poller.register(worker, Poller.POLLIN | Poller.POLLERR);
    while (!Thread.currentThread().isInterrupted()) {
      int poll = poller.poll(pollTime);
      if (poll > 0) {
        lastActiveTime = System.currentTimeMillis();
        processRequest(worker);
      } else {
        checkTimeoutHanlder();
      }

    }
  }

  private void checkTimeoutHanlder() {
    if (System.currentTimeMillis() - lastActiveTime > timeout) {
      Collection<Entry<Short, ServerHandler>> entries = Collections
          .unmodifiableCollection(handlerMap.entrySet());
      for (Entry<Short, ServerHandler> entry : entries) {
        ServerHandler value = entry.getValue();
        boolean b = System.currentTimeMillis() - value.getLastActiveTime() > timeout;
        if (b) {
          value.close();
        }
      }
    }
  }

  private void processRequest(Socket worker) {
    //  The DEALER socket gives us the address envelope and message
    ZMsg msg = ZMsg.recvMsg(worker);
    ZFrame peek = msg.peek();
    Short identity = ByteBuffer.wrap(peek.getData()).getShort();
    ServerHandler serverHandler = handlerMap.get(identity);
    if (serverHandler == null) {
      serverHandler = new ServerHandlerImpl();
      handlerMap.put(identity, serverHandler);
    }
    serverHandler.updateActiveTime();
    ZFrame zFrame1 = msg.pollLast();
    byte[] content = zFrame1.getData();
    zFrame1.destroy();
    ZFrame zFrame = msg.getLast();
    int clientId = zFrame.getData()[0] & 0xff;
    if (clientId == 0) {
      msg.addLast(content);
      msg.send(worker, true);
      return;
    }
    serverHandler.onRevc(msg, clientId, content, worker);
  }
}