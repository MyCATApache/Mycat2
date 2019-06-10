package io.mycat.rpc;

import io.mycat.rpc.publisher.Publisher;
import io.mycat.rpc.publisher.PublisherHandler;
import io.mycat.rpc.publisher.PublisherSession;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

public class RpcContextImpl2 {

  public static void main(String[] args) {
    Publisher context = new Publisher(100,new ZContext());
    int receiver = context.createReceiver("tcp://localhost:5555", new PublisherHandler() {
      @Override
      public void pollIn(PublisherSession session, Socket socket, Publisher rpc) {
        String s = socket.recvStr();
        System.out.println(s);
      }

      @Override
      public void pollErr(PublisherSession session, Socket socket, Publisher rpc) {

      }
    });
    context.process(true);
  }
}