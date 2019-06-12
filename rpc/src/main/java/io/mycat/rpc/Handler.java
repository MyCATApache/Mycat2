package io.mycat.rpc;

import io.mycat.rpc.publisher.PublisherProvider;

/**
 * The interface Handler.
 *
 * @param <T> the type parameter
 */
public interface Handler<T> {

  /**
   * Poll err.
   *
   * @param socket the socket
   * @param publisherLoop the publisher loop
   * @param message the message
   */
  void pollErr(RpcSocket socket, PublisherProvider publisherLoop, String message);
}