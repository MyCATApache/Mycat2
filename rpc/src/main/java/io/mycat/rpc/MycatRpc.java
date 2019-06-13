package io.mycat.rpc;

import io.mycat.rpc.publisher.ConsumerHandler;
import io.mycat.rpc.publisher.PublisherProvider;
import java.util.concurrent.TimeUnit;
import org.zeromq.ZContext;

public enum MycatRpc {
  INSTANCE;
  final ZContext context = new ZContext();
  PublisherProvider publisherProvider;
  final Thread thread = new Thread(() -> publisherProvider.loop());

  MycatRpc() {
    publisherProvider = new PublisherProvider(TimeUnit.SECONDS.toMillis(3),
        context);
  }

  public RpcSocket createPublisher(String addr, boolean bind) {
    return publisherProvider.createPublisher(context, addr, bind);
  }

  public void addReceiver(String addr, boolean bind, byte[] topic, ConsumerHandler handler) {
    publisherProvider.addReceiver(addr, bind, topic, handler);
  }

  public void startPublisherReceiver() {
    thread.start();
  }
}