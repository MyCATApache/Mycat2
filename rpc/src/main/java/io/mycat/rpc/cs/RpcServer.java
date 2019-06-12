package io.mycat.rpc.cs;

import java.io.Closeable;
import java.util.Arrays;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

/**
 * The type Rpc server.
 */
public class RpcServer implements Closeable {

  private final Poller poller;
  private final Socket worker;
  private ZContext ctx;
  private String addr;
  private boolean bind;
  private long timeout;
  private RpcServerSessionHandler handler;
  private long lastActiveTime;
  private RpcSocketImpl rpcSocket = new RpcSocketImpl();

  /**
   * Instantiates a new Rpc server.
   *
   * @param ctx the ctx
   * @param addr "inproc://backend"
   * @param bind the bind
   * @param handler the handler
   */
  public RpcServer(ZContext ctx, String addr, boolean bind, RpcServerSessionHandler handler
  ) {
    this.ctx = ctx;
    this.addr = addr;
    this.bind = bind;
    this.handler = handler;
    this.timeout = timeout;
    worker = ctx.createSocket(SocketType.DEALER);
    if (bind) {
      String[] addrList = addr.split(",");
      Arrays.stream(addrList).forEach(worker::bind);
    } else {
      worker.connect(addr);
    }
    poller = ctx.createPoller(1);
    poller.register(worker, Poller.POLLIN | Poller.POLLERR);
  }

  /**
   * Process.
   *
   * @param pollTime the poll time
   */
  public void process(long pollTime) {
    pollTime = timeout < pollTime ? timeout : pollTime;
    while (!Thread.currentThread().isInterrupted()) {
      int poll = poller.poll(pollTime);
      lastActiveTime = System.currentTimeMillis();
      processRequest(worker);
    }
  }

  private void processRequest(Socket worker) {
    //  The DEALER socket gives us the address envelope and message
    ZMsg msg = ZMsg.recvMsg(worker);
    handler.onUpdateActiveTime(this.lastActiveTime);
    byte[] data = msg.getLast().getData();
    rpcSocket.setFrames(msg);
    rpcSocket.setSocket(worker);
    handler.onRevc(data,rpcSocket);
  }

  @Override
  public void close() {
    poller.unregister(worker);
    worker.setLinger(0);
    worker.close();
  }
}