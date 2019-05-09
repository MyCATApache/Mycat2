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

import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.packet.PacketSplitterImpl;
import io.mycat.proxy.session.MySQLClientSession;

import io.mycat.util.MySQLUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class LoadDataFileContentTask implements ResultSetTask {
    private MySQLClientSession mysql;
    private FileChannel fileChannel;
    private int position;
    int length;
    int allRemains;
    private AsynTaskCallBack<MySQLClientSession> callBack;
    int curRemains = 0;
    //byte packetId = 2;
    PacketSplitter packetSplitter = new PacketSplitterImpl();

    public void request(MySQLClientSession mysql, FileChannel fileChannel, int position, int length, AsynTaskCallBack<MySQLClientSession> callBack) {
        try {
            this.mysql = mysql;
            this.fileChannel = fileChannel;
            this.position = position;
            this.length = length;
            this.allRemains = length;
            this.callBack = callBack;
            mysql.switchNioHandler(this);
            mysql.setCallBack(callBack);
            packetSplitter.init(length);
            packetSplitter.nextPacketInPacketSplitter();
            curRemains = packetSplitter.getPacketLenInPacketSplitter();
            writeData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeData() {
        try {
            long res;
            while (curRemains > 0) {
                SocketChannel channel = mysql.channel();
                curRemains = packetSplitter.getPacketLenInPacketSplitter();
                channel.write(ByteBuffer.wrap(MySQLUtil.getFixIntByteArray(3, curRemains)));
                byte b = mysql.incrementPacketIdAndGet();
                channel.write(ByteBuffer.wrap(new byte[]{b}));
                res =  fileChannel.transferTo(position + packetSplitter.getOffsetInPacketSplitter(),curRemains,channel);
                allRemains -= res;
                curRemains -= res;
                if (allRemains == 0 && curRemains == 0) {
                    onWriteFinished(mysql);
                    break;
                } else if (allRemains > 0 && curRemains == 0) {
                    if (packetSplitter.nextPacketInPacketSplitter()) {
                        curRemains = packetSplitter.getPacketLenInPacketSplitter();
                        continue;
                    }
                } else {
                    mysql.change2WriteOpts();
                    break;
                }
            }

        } catch (IOException e) {
            callBack.finished(mysql, this, false, null, e.getMessage());
        }
    }

    @Override
    public void onSocketWrite(MySQLClientSession mysql) throws IOException {
        onWriteFinished(mysql);
    }

    @Override
    public void onWriteFinished(MySQLClientSession mysql) throws IOException {
        if (mysql.getCurNIOHandler() == this) {
            if (this.allRemains == 0) {
                fileChannel.close();
                mysql.change2ReadOpts();
                clearAndFinished(mysql,true, null);
            } else {
                writeData();
            }
        }
    }

    @Override
    public void onFinished(MySQLClientSession mysql,boolean success, String errorMessage) {
        MySQLClientSession currentMySQLSession = mysql;
        AsynTaskCallBack<MySQLClientSession> callBack = currentMySQLSession.getCallBackAndReset();
        callBack.finished(currentMySQLSession, this, success, null, errorMessage);

    }
}
