/**
 * Copyright (C) <2021>  <jamie12221>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.callback;

import io.mycat.MycatException;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import lombok.NonNull;

/**
 * @author jamie12221
 *  date 2019-05-21 01:21
 **/
public interface TaskCallBack<T extends TaskCallBack> {

  default Exception toExpection(@NonNull ErrorPacketImpl errorPacket) {
    byte[] errorMessage = errorPacket.getErrorMessage();
    return new MycatException(new String(errorMessage));
  }
}
