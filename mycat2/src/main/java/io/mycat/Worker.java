package io.mycat;

import io.mycat.rpc.RpcSocket;
import io.mycat.rpc.cs.RpcClientHandler;
import io.mycat.rpc.cs.RpcConnectionPool;
import io.mycat.rpc.cs.RpcProvider;
import io.mycat.rpc.cs.RpcServer;
import io.mycat.rpc.cs.RpcServerSessionHandler;
import java.util.Arrays;
import org.zeromq.ZContext;

public class Worker {
 static final RpcConnectionPool connectionPool;
  static {
    ZContext context = new ZContext();
    RpcProvider provider = new RpcProvider(context);
    String serverAddress = "inproc://localhost:5570";
    provider.startProxy(Arrays.asList(serverAddress), Arrays.asList("inproc://localhost:backend"));
    provider.startServer("inproc://localhost:backend", false, 100, new RpcServerSessionHandler() {
      @Override
      public void onRevc(byte[] data, RpcSocket worker, RpcServer server) {
        worker.send(data);
        worker.destory();
      }

      @Override
      public void clear() {

      }
    });
     connectionPool = new RpcConnectionPool("1", 100, context);
    connectionPool.start();
  }
  public static  void request(RpcClientHandler clientHandler){
    connectionPool.request("inproc://localhost:5570",clientHandler);
  }
}