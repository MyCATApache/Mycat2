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
package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;

public interface QueryResultSetTask extends ResultSetTask {

    default public void request(MySQLSession mysql, String sql, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, 3, sql,callBack);
    }

    @Override
    public abstract void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos);

    @Override
    public abstract void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos);

    @Override
    public abstract void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos);

}
