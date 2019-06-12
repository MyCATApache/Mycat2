/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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