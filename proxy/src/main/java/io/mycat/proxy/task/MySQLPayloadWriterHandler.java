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

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class MySQLPayloadWriterHandler extends AbstractPayloadWriter<MySQLPacket> {
    @Override
    protected int writePayload(MySQLPacket buffer, int writeIndex, int reminsPacketLen, SocketChannel serverSocket) throws IOException {
        return serverSocket.write(buffer.currentBuffer().currentByteBuffer());
    }

    @Override
    void clearResource(MySQLPacket f) throws Exception {
        f.reset();
    }
}
