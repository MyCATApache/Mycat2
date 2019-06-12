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

/**
 * The type Abstract rpc client handler.
 */
public abstract class AbstractRpcClientHandler {

  /**
   * Get send msg byte [ ].
   *
   * @return the byte [ ]
   */
  public abstract byte[] getSendMsg();

  /**
   * On has send data.
   */
  protected abstract void onHasSendData();

  /**
   * On revc boolean.
   *
   * @param data the data
   * @return the boolean
   */
  abstract public boolean onRevc(byte[] data);

  /**
   * On wait for response timeout.
   */
  abstract public void onWaitForResponseTimeout();

  /**
   * On wait for response err.
   *
   * @param message the message
   */
  abstract public void onWaitForResponseErr(String message);

  /**
   * On wait for pending err.
   */
  abstract public void onWaitForPendingErr();


  /**
   * Gets timeout.
   *
   * @return the timeout
   */
  public abstract long getTimeout();

  /**
   * On before send data error.
   */
  abstract void onBeforeSendDataError();
}