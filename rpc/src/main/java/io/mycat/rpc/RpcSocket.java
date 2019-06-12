package io.mycat.rpc;

/**
 * The interface Rpc socket.
 */
public interface RpcSocket  {

  /**
   * Send.
   *
   * @param data the data
   */
  public void send(byte[] data);

  /**
   * Send.
   *
   * @param data the data
   */
  public default void send(String data){
    send(data.getBytes());
  }
}