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
package io.mycat.proxy.task.prepareStatement;

import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.ResultSetTask;

import java.io.IOException;

public class CloseTask implements ResultSetTask {
    public void request(MySQLSession mysql, long statementId, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, 0x19,statementId, callBack);
    }

    @Override
    public void onWriteFinished(MySQLSession mysql) throws IOException {
        clearAndFinished(true,null);
    }
}
