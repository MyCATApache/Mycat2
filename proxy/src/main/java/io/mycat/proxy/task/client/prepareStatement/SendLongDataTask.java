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
package io.mycat.proxy.task.client.prepareStatement;

import io.mycat.beans.mysql.MySQLPreparedStatement;
import io.mycat.beans.mysql.packet.PacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.ResultSetTask;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SendLongDataTask implements ResultSetTask {
    byte packetId = 0;
    PacketSplitter splitter;
    MySQLPreparedStatement preparedStatement;

    public void request(MySQLClientSession mysql, MySQLPreparedStatement preparedStatement, AsynTaskCallBack<MySQLClientSession> callBack){
        if (packetId == 0) {
            MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
            ProxyBufferImpl proxyBuffer = new ProxyBufferImpl(reactorThread.getBufPool());
            proxyBuffer.newBuffer();
            mysql.setCurrentProxyBuffer( proxyBuffer);
            splitter = new PacketSplitterImpl();
            mysql.setCallBack(callBack);
            mysql.switchNioHandler(this);
            this.preparedStatement = preparedStatement;
        }
        sendData(mysql);
    }

    private void sendData(MySQLClientSession mysql) {
        try {
            Set<Map.Entry<Integer, ByteArrayOutputStream>> entries = this.preparedStatement.getLongDataMap().entrySet();
            ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
            Iterator<Map.Entry<Integer, ByteArrayOutputStream>> iterator = entries.iterator();
            if (iterator.hasNext()) {
                Map.Entry<Integer, ByteArrayOutputStream> next = iterator.next();
                ByteArrayOutputStream value1 = next.getValue();
                byte[] data = value1.toByteArray();
                int payloadLength = data.length;
                splitter.init(payloadLength);
                while (splitter.nextPacketInPacketSplitter()) {
                    int offset = splitter.getOffsetInPacketSplitter();
                    int packetLen = splitter.getPacketLenInPacketSplitter();
                    MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
                    mySQLPacket.writeFixInt(3, packetLen);
                    mySQLPacket.writeByte(packetId);
                    mySQLPacket.writeBytes(data, offset, packetLen);
                }
                value1.close();
                iterator.remove();
                int startIndex = proxyBuffer.channelWriteStartIndex();
                proxyBuffer.channelWriteEndIndex(startIndex);
                proxyBuffer.channelWriteStartIndex(0);
                mysql.writeProxyBufferToChannel(proxyBuffer);
            } else {
                clearAndFinished(mysql,true, null);
            }
        }catch (Exception e){
            clearAndFinished(mysql,false,e.getMessage() );
        }
    }

    @Override
    public void onWriteFinished(MySQLClientSession mysql) throws IOException {
        if (mysql.getCurNIOHandler() == this){
            sendData(mysql);
        }
    }
}
