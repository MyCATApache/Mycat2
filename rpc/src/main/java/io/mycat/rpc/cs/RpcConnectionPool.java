/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.rpc.cs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Poller;

/**
 * The type Rpc connection pool.
 */
public class RpcConnectionPool {

  private String name;
  private ZContext context;
  private Poller poller;
  /**
   * The Live.
   */
  final HashMap<String, LinkedList<RpcConnection>> live = new HashMap<>();
  /**
   * The All sessions.
   */
  final ArrayList<RpcConnection> allSessions = new ArrayList<>();
  private final ConcurrentLinkedQueue<Runnable> pending = new ConcurrentLinkedQueue<>();
  private int requestCountor = 0;
  private long pollTime;
  private final Thread thread = new Thread(this::poll);

  /**
   * Instantiates a new Rpc connection pool.
   *
   * @param name the name
   * @param pollTime the poll time
   * @param context the context
   */
  public RpcConnectionPool(String name, long pollTime, ZContext context) {
    this.name = name;
    this.pollTime = pollTime;
    this.context = context;
  }

  private static LinkedList<RpcConnection> apply(String k) {
    return new LinkedList<>();
  }


  /**
   * Request.
   *
   * @param address the address
   * @param clientHandler the client handler
   */
  public void request(String address, RpcClientHandler clientHandler) {
    pending.offer(() -> {
      pending(address, clientHandler);
    });
  }

  /**
   * Start.
   */
  public void start() {
    poller = context.createPoller(1);
    this.thread.start();
  }


  private void pending(String address, RpcClientHandler clientHandler) {
    LinkedList<RpcConnection> queue = live.computeIfAbsent(address, RpcConnectionPool::apply);
    if (queue.isEmpty()) {
      RpcConnection connection = createConnection(address);
      queue.addLast(connection);
      allSessions.add(connection);
    }
    for (RpcConnection connect : queue) {
      if (connect.pending(clientHandler)){
        return;
      }
    }
    clientHandler.onBeforeSendDataError();
  }

  private RpcConnection createConnection(String address) {
    RpcConnection connection = new RpcConnection(RpcProvider.ID_PROVIDER.incrementAndGet(), address,
        context, poller, this);
    connection.connect();
    return connection;
  }

  /**
   * Recycle.
   *
   * @param connection the connection
   */
  public void recycle(RpcConnection connection) {
    pending.offer(() -> {
      LinkedList<RpcConnection> queue = live.get(connection.getAddr());
      queue.offer(connection);
    });
  }

  /**
   * Close.
   *
   * @param connection the connection
   * @param message the message
   */
  public void close(RpcConnection connection,String message) {
    pending.offer(()->{
      connection.close(message);
    });
  }

  /**
   * Poll.
   */
  public void poll() {
    Thread thread = Thread.currentThread();
    try {
      while (!thread.isInterrupted()) {
        if (!pending.isEmpty()) {
          Iterator<Runnable> iterator = pending.iterator();
          while (iterator.hasNext()) {
            Runnable next = iterator.next();
            iterator.remove();
            next.run();
          }
        }
        int poll = poller.poll(requestCountor > 0 ? 0 : pollTime);
        requestCountor = 0;
        for (RpcConnection connection : allSessions) {
          connection.process(poll);
          requestCountor += connection.getPendingRequestCount();
        }
      }
    }catch (Exception e){
      e.printStackTrace();
    }
  }
}