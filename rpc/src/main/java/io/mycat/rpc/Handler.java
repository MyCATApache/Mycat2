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

package io.mycat.rpc;

import io.mycat.rpc.publisher.PublisherProvider;

/**
 * The interface Handler.
 * @author jamie12221
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