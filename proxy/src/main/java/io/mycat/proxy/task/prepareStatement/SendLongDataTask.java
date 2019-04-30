package io.mycat.proxy.task.prepareStatement;

import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.packet.PacketSplitterImpl;
import io.mycat.proxy.session.MySQLSession;
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
    PreparedStatement preparedStatement;

    public void request(MySQLSession mysql, PreparedStatement preparedStatement, AsynTaskCallBack<MySQLSession> callBack){
        if (packetId == 0) {
            MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
            ProxyBufferImpl proxyBuffer = new ProxyBufferImpl(reactorThread.getBufPool());
            proxyBuffer.newBuffer();
            mysql.setProxyBuffer( proxyBuffer);
            splitter = new PacketSplitterImpl();
            mysql.setCallBack(callBack);
            mysql.switchNioHandler(this);
            this.preparedStatement = preparedStatement;
        }
        sendData(mysql);
    }

    private void sendData(MySQLSession mysql) {
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
                mysql.writeToChannel(proxyBuffer);
            } else {
                clearAndFinished(true, null);
            }
        }catch (Exception e){
            clearAndFinished(false,e.getMessage() );
        }
    }

    @Override
    public void onWriteFinished(MySQLSession mysql) throws IOException {
        if (mysql.getCurNIOHandler() == this){
            sendData(mysql);
        }
    }
}
