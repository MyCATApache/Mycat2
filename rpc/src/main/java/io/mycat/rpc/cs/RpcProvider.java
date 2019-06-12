package io.mycat.rpc.cs;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.zeromq.ZContext;

/**
 * The type Rpc provider.
 */
public class RpcProvider {

  /**
   * The Thread pool.
   */
  final  ExecutorService threadPool = Executors.newCachedThreadPool();
  private ZContext context;
  /**
   * The Id provider.
   */
  static final AtomicInteger ID_PROVIDER = new AtomicInteger(0);

  /**
   * Instantiates a new Rpc provider.
   *
   * @param context the context
   */
  public RpcProvider(ZContext context) {

    this.context = context;
  }

  /**
   * Start proxy.
   *
   * @param serverAddrList the server addr list
   * @param backendAddrList the backend addr list
   */
  public void startProxy(List<String> serverAddrList, List<String> backendAddrList) {
    threadPool.submit(new RpcProxy(context, serverAddrList, backendAddrList));
  }

  /**
   * Start server.
   *
   * @param addr the addr
   * @param bind the bind
   * @param timeout the timeout
   * @param sessionHandler the session handler
   */
  public void startServer(String addr, boolean bind, int timeout,
      RpcServerSessionHandler sessionHandler) {
    threadPool.submit(() -> {
      try (RpcServer serverWorker = new RpcServer(context, addr, bind, sessionHandler)) {
        serverWorker.process(timeout);
      } catch (Exception e) {
        e.printStackTrace();
      }finally {
        sessionHandler.clear();
      }
    });
  }

}
