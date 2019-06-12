package io.mycat.rpc.publisher;

import io.mycat.rpc.RpcSocket;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

/**
 * The type Publish socket.
 */
public class PublishSocket implements RpcSocket {

  /**
   * The Socket.
   */
  Socket socket;

  /**
   * Gets socket.
   *
   * @return the socket
   */
  public Socket getSocket() {
    return socket;
  }

  /**
   * Sets socket.
   *
   * @param socket the socket
   */
  public void setSocket(Socket socket) {
    this.socket = socket;
  }

  @Override
  public void send(byte[] data) {
    this.socket.send(data, ZMQ.NOBLOCK);
  }
}