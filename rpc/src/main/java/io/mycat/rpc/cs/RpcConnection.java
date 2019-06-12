package io.mycat.rpc.cs;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;


/**
 * The type Rpc connection.
 */
public class RpcConnection {

  private byte[] identity;
  private String addr;
  private ZContext ctx;
  private Poller poller;
  private RpcConnectionPool rpcConnectionPool;
  private Socket client;
  private int pollId;
  private volatile AbstractRpcClientHandler waitForRecv;
  private final AtomicLong globalLastActiveTime = new AtomicLong(0);
  private final ConcurrentLinkedQueue<AbstractRpcClientHandler> pending = new ConcurrentLinkedQueue<>();
  private final AtomicInteger pendingCount = new AtomicInteger(0);

  /**
   * Instantiates a new Rpc connection.
   *
   * @param identity the identity
   * @param addr the addr
   * @param ctx the ctx
   * @param poller the poller
   * @param rpcConnectionPool the rpc connection pool
   */
  RpcConnection(int identity, String addr, ZContext ctx, Poller poller,
      RpcConnectionPool rpcConnectionPool) {
    this.identity = ByteBuffer.allocate(4).putInt(identity).array();
    this.addr = addr;
    this.ctx = ctx;
    this.poller = poller;
    this.rpcConnectionPool = rpcConnectionPool;
    this.globalLastActiveTime.set(Long.MAX_VALUE);
  }

  /**
   * Pending boolean.
   *
   * @param clientHandler the client handler
   * @return the boolean
   */
  boolean pending(AbstractRpcClientHandler clientHandler) {
    if (System.currentTimeMillis() - globalLastActiveTime.get() < clientHandler.getTimeout()) {
      pendingCount.incrementAndGet();
      pending.add(clientHandler);
      return true;
    } else {
      return false;
    }
  }


  /**
   * "inproc://localhost:5570"
   */
  void connect() {
    client = ctx.createSocket(SocketType.DEALER);
    client.setIdentity(identity);
    client.connect(addr);
    pollId = poller.register(client, Poller.POLLIN | Poller.POLLERR);

  }

  /**
   * Process.
   *
   * @param poll the poll
   */
  void process(long poll) {
    if (poller.pollerr(pollId)) {
      globalLastActiveTime.set(Long.MAX_VALUE);
      close(ZMQ.Error.findByCode(this.client.errno()).getMessage());
      return;
    }
    long now = System.currentTimeMillis();
    if (poller.pollin(pollId)) {
      globalLastActiveTime.set(now);

      ZMsg msg = ZMsg.recvMsg(client);
      try {
        byte[] data = msg.getLast().getData();
        if (data.length != 0) {
          boolean b = waitForRecv.onRevc(data);
          if (b) {
            waitForRecv = null;
          }
        }
      } catch (Exception e) {
        waitForRecv.onWaitForResponseErr(e.toString());
        waitForRecv = null;
      } finally {
        msg.destroy();
      }
    }

    if (waitForRecv == null && poll <= 0) {
      client.send(new byte[]{}, ZMQ.NOBLOCK);
    }

    if (waitForRecv != null) {
      if (now - globalLastActiveTime.get() > waitForRecv.getTimeout()) {
        AbstractRpcClientHandler handler = this.waitForRecv;
        this.waitForRecv = null;
        handler.onWaitForResponseTimeout();
      }
    }
    while (this.waitForRecv == null&&!pending.isEmpty()) {
      AbstractRpcClientHandler handler = pending.poll();
      if (now - globalLastActiveTime.get() < handler.getTimeout()) {
        pendingCount.decrementAndGet();
        client.send(handler.getSendMsg(), ZMQ.NOBLOCK);
        waitForRecv = handler;
        handler.onHasSendData();
      } else {
        handler.onBeforeSendDataError();
      }
    }

  }

  /**
   * Close.
   *
   * @param message the message
   */
  void close(String message) {
    RpcConnectionPool pool = this.rpcConnectionPool;
    pool.allSessions.remove(this);
    pool.live.get(getAddr()).remove(this);
    for (AbstractRpcClientHandler clientHandler : this.pending) {
      clientHandler.onWaitForPendingErr();
    }
    if (waitForRecv != null) {
      waitForRecv.onWaitForResponseErr(message);
    }
    poller.unregister(client);
    client.setLinger(0);
    client.close();
  }

  /**
   * Getter for property 'addr'.
   *
   * @return Value for property 'addr'.
   */
  public String getAddr() {
    return addr;
  }

  /**
   * Gets pending request count.
   *
   * @return the pending request count
   */
  public int getPendingRequestCount() {
    return pendingCount.get();
  }
}