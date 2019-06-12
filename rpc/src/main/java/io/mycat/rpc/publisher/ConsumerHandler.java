package io.mycat.rpc.publisher;

import io.mycat.rpc.Handler;

/**
 * The interface Consumer handler.
 */
public interface ConsumerHandler extends Handler<PublisherProvider> {

  /**
   * Poll in.
   *
   * @param bytes the bytes
   * @param rpc the rpc
   */
  public void pollIn(byte[] bytes, PublisherProvider rpc);


}