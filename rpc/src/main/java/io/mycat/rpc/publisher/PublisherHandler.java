package io.mycat.rpc.publisher;

import org.zeromq.ZMQ.Socket;

public interface PublisherHandler {

  public default void pollIn(PublisherSession session, Socket socket, Publisher rpc){}

  public default void pollOut(PublisherSession session, Socket socket, Publisher rpc){}

  public  void pollErr(PublisherSession session, Socket socket, Publisher rpc);
}