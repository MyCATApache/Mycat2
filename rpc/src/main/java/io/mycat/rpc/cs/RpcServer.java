/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.rpc.cs;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
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
  private final LinkedList<Runnable> pending = new LinkedList<>();

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
      if (!pending.isEmpty()) {
        Iterator<Runnable> iterator = pending.iterator();
        while (iterator.hasNext()) {
          Runnable runnable = iterator.next();
          iterator.remove();
          runnable.run();
        }
      }
    }
  }

  private void processRequest(Socket worker) {
    //  The DEALER socket gives us the address envelope and message
    ZMsg msg = ZMsg.recvMsg(worker);
    handler.onUpdateActiveTime(this.lastActiveTime);
    byte[] data = msg.getLast().getData();
    if (data.length==0){
      msg.send(worker,true);
      return;
    }
    RpcSocketImpl rpcSocket = new RpcSocketImpl();
    rpcSocket.setFrames(msg);
    rpcSocket.setSocket(worker);
    handler.onRevc(data, rpcSocket, this);
  }

  @Override
  public void close() {
    if (handler != null) {
      handler.clear();
    }
    poller.unregister(worker);
    worker.setLinger(0);
    worker.close();
  }

  public void pending(Runnable runnable) {
    pending.addLast(runnable);
  }
}