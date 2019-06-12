package io.mycat.rpc.cs;

import io.mycat.rpc.RpcSocket;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

/**
 * The type Rpc socket.
 */
public class RpcSocketImpl implements RpcSocket {

  /**
   * The Frames.
   */
  ZMsg frames;
  /**
   * The Socket.
   */
  Socket socket;

  /**
   * Gets frames.
   *
   * @return the frames
   */
  public ZMsg getFrames() {
    return frames;
  }

  /**
   * Sets frames.
   *
   * @param frames the frames
   */
  public void setFrames(ZMsg frames) {
    this.frames = frames;
  }

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
    frames.addLast(data);
    frames.send(socket,true);
  }
}