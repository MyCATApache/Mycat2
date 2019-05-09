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
import io.mycat.proxy.session.MySQLClientSession;

public class MultiOkQueriesCounterTask implements QueryResultSetTask {
    private int counter = 0;

    public MultiOkQueriesCounterTask(int counter) {
        this.counter = counter;
    }


    @Override
    public void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onFinished(MySQLClientSession mysql,boolean success, String errorMessage) {
        if (counter == 0) {
            AsynTaskCallBack<MySQLClientSession> callBack =mysql.getCallBackAndReset();
            callBack.finished(mysql, this, true, null, errorMessage);
        } else {
            AsynTaskCallBack<MySQLClientSession> callBack = mysql.getCallBackAndReset();
            callBack.finished(mysql, this, false, null,success? "couter fail":errorMessage);
        }
    }

    @Override
    public void onOk(MySQLPacket mySQLPacket, int startPos, int endPos) {
        counter--;
    }

    @Override
    public void onColumnCount(int columnCount) {

    }
}
