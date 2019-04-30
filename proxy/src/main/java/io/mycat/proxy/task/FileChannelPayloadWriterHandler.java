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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class FileChannelPayloadWriterHandler extends AbstractPayloadWriter<FileChannel> {
    @Override
    protected int writePayload(FileChannel fileChannel, int writeIndex, int reminsPacketLen, SocketChannel serverSocket) throws IOException {
        return (int) fileChannel.transferTo(writeIndex, reminsPacketLen, serverSocket);
    }

    @Override
    void clearResource(FileChannel f) throws Exception {
        f.close();
    }
}
