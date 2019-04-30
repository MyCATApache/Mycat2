package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.packet.PacketSplitterImpl;
import io.mycat.proxy.session.MySQLSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class LoadDataFileContentTask implements ResultSetTask {
    private MySQLSession mysql;
    private FileChannel fileChannel;
    private int position;
    int length;
    int allRemains;
    private AsynTaskCallBack<MySQLSession> callBack;
    int curRemains = 0;
    //byte packetId = 2;
    PacketSplitter packetSplitter = new PacketSplitterImpl();

    public void request(MySQLSession mysql, FileChannel fileChannel, int position, int length, AsynTaskCallBack<MySQLSession> callBack) {
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
                channel.write(ByteBuffer.wrap(MySQLPacket.getFixIntByteArray(3, curRemains)));
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
    public void onSocketWrite(MySQLSession mysql) throws IOException {
        onWriteFinished(mysql);
    }

    @Override
    public void onWriteFinished(MySQLSession mysql) throws IOException {
        if (mysql.getCurNIOHandler() == this) {
            if (this.allRemains == 0) {
                fileChannel.close();
                mysql.change2ReadOpts();
                clearAndFinished(true, null);
            } else {
                writeData();
            }
        }
    }

    @Override
    public void onFinished(boolean success, String errorMessage) {
        MySQLSession currentMySQLSession = getCurrentMySQLSession();
        AsynTaskCallBack<MySQLSession> callBack = currentMySQLSession.getCallBackAndReset();
        callBack.finished(currentMySQLSession, this, success, null, errorMessage);

    }
}
