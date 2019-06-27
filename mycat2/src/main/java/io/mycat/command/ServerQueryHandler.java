package io.mycat.command;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.ReactorEnv;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MycatSession;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ServerQueryHandler {
  final static ExecutorService service = Executors.newCachedThreadPool(new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      return new ReactorEnvThread(r) {
      };
    }
  });
  public ServerQueryHandler(ProxyRuntime runtime) {

  }

  public void doQuery(byte[] sqlBytes, MycatSession mycatSession) {

    service.submit(()->{
      try {
        ReactorEnvThread thread = (ReactorEnvThread) Thread.currentThread();
        mycatSession.deliverWorkerThread(thread);
        try {
          mycatSession.writeOkEndPacket();
        } finally {
          mycatSession.backFromWorkerThread(thread);
        }
      }catch (Exception e){
        e.printStackTrace();
      }
    });
  }
}