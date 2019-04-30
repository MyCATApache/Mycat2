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
package io.mycat.proxy;

import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.session.MycatSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MainMySQLNIOHandler implements NIOHandler<MySQLSession> {
    public static final MainMySQLNIOHandler INSTANCE = new MainMySQLNIOHandler();
    protected final static Logger logger = LoggerFactory.getLogger(MainMySQLNIOHandler.class);
    @Override
    public void onSocketRead(MySQLSession session) throws IOException {
        MycatSession mycatSession = session.getMycatSession();
        MySQLCommand curSQLCommand = session.getMycatSession().getCurSQLCommand();
        if (curSQLCommand == null){
            System.out.println();
        }
        if(curSQLCommand.onBackendResponse(session)){
            curSQLCommand.clearResouces(session,session.isClosed());
            mycatSession.switchSQLCommand(null);
        }
    }

    @Override
    public void onSocketWrite(MySQLSession session) throws IOException {
            session.writeToChannel();
    }

    @Override
    public void onWriteFinished(MySQLSession session) throws IOException {
        MycatSession mycatSession = session.getMycatSession();
        MySQLCommand curSQLCommand = session.getMycatSession().getCurSQLCommand();
        if(curSQLCommand.onBackendWriteFinished(session)){
            curSQLCommand.clearResouces(session,session.isClosed());
            mycatSession.switchSQLCommand(null);
        }
    }

    @Override
    public void onSocketClosed(MySQLSession session, boolean normal) {
        logger.info("MySQL Session closed normal:{} ,{}" ,normal,session);
        // 交给SQLComand去处理
        MycatSession mycatSession = session.getMycatSession();
        MySQLCommand curCmd = mycatSession.getCurSQLCommand();
        try {
            if (curCmd.onBackendClosed(session, normal)) {
                curCmd.clearResouces(mycatSession, session.isClosed());
                mycatSession.switchSQLCommand(null);
            }
        } catch (IOException e) {
            logger.warn("MySQL Session {} onSocketClosed caught err ",session, e);
          //  mycatSession.closeAllBackendsAndResponseError(false, ErrorCode.ER_ERROR_ON_CLOSE, e.toString());

        }
    }
}
