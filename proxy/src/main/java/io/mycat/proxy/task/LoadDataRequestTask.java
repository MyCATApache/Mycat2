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

public class LoadDataRequestTask implements ResultSetTask {
    String fileName;

    @Override
    public void onFinished(boolean success, String errorMessage) {
        MySQLSession currentMySQLSession = getCurrentMySQLSession();
        AsynTaskCallBack<MySQLSession> callBack = currentMySQLSession.getCallBackAndReset();
        callBack.finished(currentMySQLSession, this, success, fileName, errorMessage);
    }

    @Override
    public void onLoadDataRequest(MySQLPacket mySQLPacket, int startPos, int endPos) {
        fileName = mySQLPacket.getEOFString(startPos + 5);
        clearAndFinished(true,null);
    }
}
