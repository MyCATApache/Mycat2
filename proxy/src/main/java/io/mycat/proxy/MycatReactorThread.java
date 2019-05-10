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

import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import io.mycat.router.MycatRouter;
import java.io.IOException;

public final class MycatReactorThread extends ProxyReactorThread<MycatSession> {
    final MySQLSessionManager mySQLSessionManager = new MySQLSessionManager();
    final PacketSplitterImpl packetSplitter = new PacketSplitterImpl();
    final MycatRouter router = new MycatRouter(MycatRuntime.INSTANCE.getRouterConfig());

    public MycatRouter getRouter() {
        return router;
    }

    public PacketSplitterImpl getPacketSplitter() {
        return packetSplitter;
    }
    public MycatReactorThread(BufferPool bufPool, FrontSessionManager<MycatSession> sessionManager) throws IOException {
        super(bufPool, sessionManager);
    }
    public MySQLSessionManager getMySQLSessionManager() {
        return mySQLSessionManager;
    }

}
