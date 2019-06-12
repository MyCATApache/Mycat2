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


/**
 * The type Rpc server session handler.
 */
public abstract class RpcServerSessionHandler {

  /**
   * The Last active time.
   */
  long lastActiveTime;

  /**
   * On revc.
   *
   * @param data the data
   * @param worker the worker
   */
  public abstract void onRevc(byte[] data, RpcSocket worker,RpcServer server);

  /**
   * Gets last active time.
   *
   * @return the last active time
   */
  public long getLastActiveTime() {
    return lastActiveTime;
  }

  /**
   * Clear.
   */
  public abstract void clear();

  /**
   * On update active time.
   *
   * @param lastActiveTime the last active time
   */
  public void onUpdateActiveTime(long lastActiveTime) {
    this.lastActiveTime = lastActiveTime;
  }

}