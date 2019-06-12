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

package io.mycat.rpc;

import io.mycat.rpc.cs.RpcClientHandler;
import io.mycat.rpc.cs.RpcConnectionPool;
import io.mycat.rpc.cs.RpcProvider;
import io.mycat.rpc.cs.RpcServer;
import io.mycat.rpc.cs.RpcServerSessionHandler;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.zeromq.ZContext;

public class Main {

  public static void main(String[] args) throws Exception {
    ZContext context = new ZContext();
    RpcProvider provider = new RpcProvider(context);
    String serverAddress = "inproc://localhost:5570";
    provider.startProxy(Arrays.asList(serverAddress), Arrays.asList("inproc://localhost:backend"));
    provider.startServer("inproc://localhost:backend", false, 1, new RpcServerSessionHandler() {
      @Override
      public void onRevc(byte[] data, RpcSocket worker, RpcServer server) {
        worker.send(data);
        worker.destory();
      }

      @Override
      public void clear() {

      }
    });
    RpcConnectionPool connectionPool = new RpcConnectionPool("1", 0, context);
    connectionPool.start();

    ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
    for (int i = 0; i < 1000000; i++) {
      connectionPool.request("inproc://localhost:5570", getClientHandler());
    }
  }

  static AtomicLong atomicLong = new AtomicLong(0);

  private static RpcClientHandler getClientHandler() {
    long l = atomicLong.incrementAndGet();
    return new RpcClientHandler() {
      byte[] request = ("" + l + "").getBytes();

      @Override
      public void onRetry() {
        System.out.println("onRetry");
      }

      @Override
      public void onResponseError(String message) {
        System.out.println(message);
      }


      @Override
      public byte[] getSendMsg() {
        return request;
      }

      @Override
      public boolean onRevc(byte[] data) {
        byte[] sendMsg = getSendMsg();
        if (!Arrays.equals(sendMsg, data)) {
          throw new RuntimeException("11111111111111111");
        }
        System.out.println("1");
        return true;
      }

      @Override
      public long getTimeout() {
        return 1000000000;
      }
    };
  }
}