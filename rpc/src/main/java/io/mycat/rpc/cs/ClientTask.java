package io.mycat.rpc.cs;

//---------------------------------------------------------------------
//This is our client task
//It connects to the server, and then sends a request once per second
//It collects responses as they arrive, and it prints them out. We will
//run several client tasks in parallel, each with a different random ID.

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;


public class ClientTask {

  private byte[] identity;
  private String addr;
  ZContext ctx;
  private Poller poller;
  private Socket client;
  private int pollId;
  private AtomicInteger pendingCount = new AtomicInteger(0);
  private ConcurrentLinkedQueue<ClientHandler> pending = new ConcurrentLinkedQueue<>();
  private HashMap<Byte, ClientHandler> waitForRecv = new HashMap<>(256);
  private long[] lastActiveTimeList = new long[256];
  private long globallastActiveTime;
  private byte seqId;

  public ClientTask(short identity, String addr, ZContext ctx, Poller poller) {
    this.identity = ByteBuffer.allocate(2).putShort(identity).array();
    this.addr = addr;
    this.ctx = ctx;
    this.poller = poller;
  }

  public boolean pending(ClientHandler clientHandler) {
    if (waitForRecv.size() < 256
        && System.currentTimeMillis() - globallastActiveTime < clientHandler.getTimeout()) {
      pendingCount.incrementAndGet();
      pending.add(clientHandler);
      if (seqId == 0) {
        seqId++;
      }
      clientHandler.setSeqId((byte) seqId++);
      return true;
    } else {
      return false;
    }
  }


  /**
   * "inproc://localhost:5570"
   */
  public void connect() {
    client = ctx.createSocket(SocketType.DEALER);
    client.setIdentity(identity);
    client.connect(addr);
    pollId = poller.register(client, Poller.POLLIN | Poller.POLLERR);

  }

  /**
   * @param pollTime 10
   */
  public void process(long pollTime, boolean loop) {
    while (!Thread.currentThread().isInterrupted()) {
      int poll = poller.poll(pollTime);
      if (poller.pollin(pollId)) {
        ZMsg msg = ZMsg.recvMsg(client);
        ZFrame sessionInfo = msg.getFirst();
        byte sessionId = sessionInfo.getData()[0];
        if (sessionId == 0) {
          globallastActiveTime = System.currentTimeMillis();
          continue;
        } else {
          ClientHandler clientHandler = waitForRecv.get(sessionId);
          if (clientHandler == null) {
            System.out.println("receice error response");
            return;
          }else {
            boolean b = clientHandler.onRevc(msg.getLast().getData());
            if (b) {
              System.out.println("remove" + this.seqId);
              waitForRecv.remove((byte) clientHandler.getSeqId());
            }
          }
          msg.destroy();
        }
      }
      if (poller.pollerr(pollId)) {
        globallastActiveTime = Long.MIN_VALUE;
        Iterator<ClientHandler> iterator = pending.iterator();
        while (iterator.hasNext()) {
          ClientHandler next = iterator.next();
          iterator.remove();
          next.onWaitForPendingErr();
        }
        iterator = waitForRecv.values().iterator();
        while (iterator.hasNext()) {
          ClientHandler next = iterator.next();
          iterator.remove();
          next.onWaitForPollErr();
        }
      }

      long now = System.currentTimeMillis();
      if (poll > 0) {
        globallastActiveTime = System.currentTimeMillis();
      } else {
        client.send(new byte[]{(byte) 0}, ZMQ.SNDMORE);
        client.send(new byte[]{(byte) 0}, 0);
        for (int i = 0; i < lastActiveTimeList.length; i++) {
          ClientHandler handler = waitForRecv.get((byte) i);
          if (handler == null) {
            continue;
          }
          long lastTime = lastActiveTimeList[i];
          if (now - lastTime > handler.getTimeout()) {
            waitForRecv.remove((byte) i);
            handler.onWaitForTimeout();
          }
        }
      }
      while (!pending.isEmpty()) {
        ClientHandler handler = pending.poll();
        if (System.currentTimeMillis() - globallastActiveTime < handler.getTimeout()) {
          pendingCount.decrementAndGet();
          client.send(new byte[]{(byte) handler.getSeqId()}, ZMQ.DONTWAIT | ZMQ.SNDMORE);
          client.send(handler.getSendMsg(), 0);
          lastActiveTimeList[handler.getSeqId()&0xff] = System.currentTimeMillis();
          waitForRecv.put((byte) handler.getSeqId(), handler);
          handler.onHasSendData();
        } else {
          handler.onBeforeSendDataError();
        }
      }
      if (!loop) {
        break;
      }
    }
  }
}