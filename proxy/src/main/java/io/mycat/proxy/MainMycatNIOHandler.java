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

import io.mycat.proxy.packet.ColumnDefPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.RouteStrategyImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MainMycatNIOHandler implements NIOHandler<MycatSession> {
    public static final MainMycatNIOHandler INSTANCE = new MainMycatNIOHandler();
    private static final Logger logger = LoggerFactory.getLogger(MainMycatNIOHandler.class);

    @Override
    public void onSocketRead(MycatSession session) throws IOException {
        session.currentProxyBuffer().newBufferIfNeed();
        if(!session.readFromChannel()){
            return;
        }
        if (!session.readMySQLPayloadFully()) return;
        MySQLPacket curPacket = session.currentPayload();
        byte head = curPacket.readByte();
        String sql = curPacket.readEOFString();
        processSQL(head, session);
    }

    private void processSQL(byte head, final MycatSession mycat) throws IOException {
        switch (head) {
            default:
                doQuery(mycat);
        }
        if (mycat.getCurSQLCommand().procssSQL(mycat)) {
            mycat.getCurSQLCommand().clearResouces(mycat, mycat.isClosed());
        }
    }

    private void doQuery(final MycatSession mycat) throws IOException {
        mycat.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
    }

    @Override
    public void onSocketWrite(MycatSession mycat) throws IOException {
        mycat.writeToChannel();
    }

    @Override
    public void onWriteFinished(MycatSession mycat) throws IOException {
        MySQLCommand command = mycat.getCurSQLCommand();
        if (command == null) {
            if (mycat.isResponseFinished()) {
                mycat.change2ReadOpts();
            } else {
                mycat.writeToChannel();
            }
        } else {
            if(command.onFrontWriteFinished(mycat)){
                command.clearResouces(mycat,mycat.isClosed());
                mycat.switchSQLCommand(null);
                mycat.resetPacket();
            }
        }
    }

    @Override
    public void onSocketClosed(MycatSession session, boolean normal) {
        session.close(normal, "");
    }

}
