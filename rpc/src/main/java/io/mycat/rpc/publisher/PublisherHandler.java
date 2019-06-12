package io.mycat.rpc.publisher;

import io.mycat.rpc.Handler;
import io.mycat.rpc.RpcSocket;

/**
 * The interface Publisher handler.
 */
public interface PublisherHandler extends Handler<PublisherProvider> {

  /**
   * Poll out.
   *
   * @param socket the socket
   * @param rpc the rpc
   */
  public  void pollOut(RpcSocket socket, PublisherProvider rpc);
}