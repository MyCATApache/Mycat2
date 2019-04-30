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
package io.mycat.proxy.session.resultSetProcesss;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.packet.ColumnDefPacket;
import io.mycat.proxy.session.MycatSession;

import java.io.IOException;

public class ResultSetNIOHandler implements NIOHandler<MycatSession> {

    @Override
    public void onSocketRead(MycatSession session) throws IOException {

    }

    @Override
    public void onWriteFinished(MycatSession session) throws IOException {

    }

    @Override
    public void onSocketClosed(MycatSession session, boolean normal) {

    }
}
