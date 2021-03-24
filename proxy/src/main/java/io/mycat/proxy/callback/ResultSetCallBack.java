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

import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;

public interface ResultSetCallBack<T> extends TaskCallBack<ResultSetCallBack<T>> {

  void onFinishedSendException(Exception exception, Object sender, Object attr);

  void onFinishedException(Exception exception, Object sender, Object attr);

  void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender, Object attr);

  void onErrorPacket(ErrorPacketImpl errorPacket,boolean monopolize, MySQLClientSession mysql, Object sender, Object attr);
  interface GetTextResultSetTransforCallBack extends
      ResultSetCallBack<GetTextResultSetTransforCallBack> {

    void onValue(int columnIndex, MySQLPacket packet, int type, int startIndex);
  }

  interface GetBinaryResultSetTransforCallBack extends
      ResultSetCallBack<GetBinaryResultSetTransforCallBack> {

    void onValue(int columnIndex, MySQLPacket packet, int type, int startIndex);

    void onNull(int columnIndex, int type, int startIndex);
  }
}