package io.mycat.rpc;

import io.mycat.rpc.cs.RpcClientHandler;
import io.mycat.rpc.cs.RpcConnectionPool;
import io.mycat.rpc.cs.RpcProvider;
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
//    provider.startProxy(Arrays.asList(serverAddress), Arrays.asList("tcp://localhost:backend"));
    provider.startServer(serverAddress, true, 1, new RpcServerSessionHandler() {
      @Override
      public void onRevc(byte[] data, RpcSocket worker) {
        worker.send(data);
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