package io.mycat.command;

import io.mycat.Worker;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MycatSession;
import io.mycat.rpc.cs.RpcClientHandler;

public class ServerQueryHandler {

  public ServerQueryHandler(ProxyRuntime runtime) {

  }

  public void doQuery(byte[] sqlBytes, MycatSession mycatSession) {
    MycatReactorThread thread = mycatSession.getMycatReactorThread();
    Worker.request(new RpcClientHandler() {
      @Override
      public void onRetry() {

      }

      @Override
      public void onResponseError(String message) {

      }

      @Override
      public byte[] getSendMsg() {
        return sqlBytes;
      }

      @Override
      public boolean onRevc(byte[] data) {
        thread.addNIOJob(()->{
          mycatSession.setLastMessage(new String(data));
          mycatSession.writeErrorEndPacket();
        });
        return true;
      }

      @Override
      public long getTimeout() {
        return 1000000000;
      }
    });
  }
}