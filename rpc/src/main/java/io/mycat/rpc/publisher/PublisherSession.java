package io.mycat.rpc.publisher;

import io.mycat.rpc.publisher.PublisherHandler;
import org.zeromq.ZMQ.Socket;

public interface PublisherSession {
  int id();
   PublisherHandler getHandler();
   Socket getSocket();
}