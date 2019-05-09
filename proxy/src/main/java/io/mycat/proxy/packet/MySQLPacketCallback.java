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
package io.mycat.proxy.packet;

import io.mycat.proxy.session.MySQLClientSession;

public interface MySQLPacketCallback {

    default void onRequest(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareLongData(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onError(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onOk(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onEof(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onColumnCount(int columnCount) {

    }

    default void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onColumnDefEof(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onRowEof(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    void onFinished(MySQLClientSession mysql,boolean success, String errorMessage);

    default void onRowOk(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onRowError(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareOk(PreparedOKPacket preparedOKPacket) {

    }

    default void onPrepareOkParameterDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareOkColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareOkColumnDefEof(EOFPacket packet) {

    }

    default void onPrepareOkParameterDefEof(EOFPacket packet) {

    }

    default void onLoadDataRequest(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }
}
