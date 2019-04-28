package io.mycat.proxy.task;

import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.session.AbstractMySQLSession;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.session.Session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class AbstractPayloadWriter<T> implements NIOHandler<AbstractMySQLSession>, PacketSplitter {
    private T buffer;
    private int startIndex;
    private int writeIndex;
    private int length;
    BufferPool bufferPool;
    SocketChannel socketChannel;
    int reminsPacketLen;
    int totalSize;
    int currentPacketLen;
    int offset;
    ByteBuffer header;
    private MySQLSession mysql;

    public void request(MySQLSession mysql, T buffer, int position, int length, AsynTaskCallBack<MySQLSession> callBack) {
        try {
            this.mysql = mysql;
            this.buffer = buffer;
            this.writeIndex = this.startIndex = position;
            this.length = length;
            mysql.setCallBack(callBack);
            this.setServerSocket(mysql.channel());
            MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
            setBufferPool(thread.getBufPool());
            this.init(length);
            mysql.switchNioHandler(this);
            onStart();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public int onPayloadLength() {
        return length;
    }


    public void onStart() {
        try {
            init(onPayloadLength());
            header = getBufferPool().allocate(4);
            boolean hasNext = false;
            while (hasNext = nextPacketInPacketSplitter()) {
                this.writeIndex = startIndex + getOffsetInPacketSplitter();
                setReminsPacketLen(getPacketLenInPacketSplitter());
                setPacketId((byte) (1 + getPacketId()));
                writeHeader(header, getPacketLenInPacketSplitter(), getPacketId());
                getServerSocket().write(header);
                if (!header.hasRemaining()) {
                    int writed = writePayload(buffer, this.writeIndex, getReminsPacketLen(), getServerSocket());
                    this.writeIndex += writed;
                    setReminsPacketLen(getReminsPacketLen() - writed);
                }
                int reminsPacketLen = getReminsPacketLen();
                if (reminsPacketLen > 0) {
                    mysql.change2WriteOpts();
                    break;
                } else if (reminsPacketLen == 0) {
                    continue;
                }
            }
            if (!hasNext) {
                writeFinishedAndClear(true);
                return;
            }
        } catch (Exception e) {
            onError(e);
        }
    }


    public void onSocketWrite(MySQLSession session) throws IOException {
        if (header.hasRemaining()) {
            getServerSocket().write(header);
            return;
        }
        int writed = writePayload(buffer, this.writeIndex, getReminsPacketLen(), getServerSocket());
        this.writeIndex += writed;
        setReminsPacketLen(getReminsPacketLen() - writed);
        while (getReminsPacketLen() == 0) {
            if (nextPacketInPacketSplitter()) {
                this.writeIndex = this.startIndex + getOffsetInPacketSplitter();
                setReminsPacketLen(getPacketLenInPacketSplitter());
                setPacketId((byte) (1 + getPacketId()));
                writeHeader(header, getPacketLenInPacketSplitter(), getPacketId());
                getServerSocket().write(header);
                if (!header.hasRemaining()) {
                    writed = writePayload(buffer, this.writeIndex, getReminsPacketLen(), getServerSocket());
                    this.writeIndex += writed;
                    setReminsPacketLen(getReminsPacketLen() - writed);
                }
            } else {
                writeFinishedAndClear(true);
                return;
            }
        }
    }

    protected abstract int writePayload(T buffer, int writeIndex, int reminsPacketLen, SocketChannel serverSocket) throws IOException ;

    public void writeFinishedAndClear(boolean success) {
        mysql.clearReadWriteOpts();
        getBufferPool().recycle(header);
        header = null;
        setServerSocket(null);
        onWriteFinished(buffer, success);
    }

    void writeHeader(ByteBuffer buffer, int packetLen, int packerId) {
        buffer.position(0);
        MySQLPacket.writeFixIntByteBuffer(buffer, 3, packetLen);
        buffer.put((byte) packerId);
        buffer.position(0);
    }

    void onWriteFinished(T fileChannel, boolean success) {
        AsynTaskCallBack callBackAndReset = getCurrentMySQLSession().getCallBackAndReset();
        try {
            clearResource(fileChannel);
            callBackAndReset.finished(this.mysql, this, success, null, null);
        } catch (Exception e) {
            mysql.setLastThrowable(e);
            callBackAndReset.finished(this.mysql, this, false, null, null);
        }
    }

     abstract  void clearResource(T f)throws Exception;

    void onError(Throwable e) {
        mysql.setLastThrowable(e);
        writeFinishedAndClear(false);
    }

    public byte getPacketId() {
        return mysql.getPacketId();
    }


    public void setPacketId(byte packetId) {
        mysql.setPacketId(packetId);
    }


    public BufferPool getBufferPool() {
        return bufferPool;
    }


    public void setBufferPool(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }


    public SocketChannel getServerSocket() {
        return socketChannel;
    }


    public void setServerSocket(SocketChannel serverSocket) {
        this.socketChannel = serverSocket;
    }


    public int getReminsPacketLen() {
        return reminsPacketLen;
    }


    public void setReminsPacketLen(int reminsPacketLen) {
        this.reminsPacketLen = reminsPacketLen;
    }


    public int getTotalSizeInPacketSplitter() {
        return totalSize;
    }


    public void setTotalSizeInPacketSplitter(int totalSize) {
        this.totalSize = totalSize;
    }


    public int getCurrentPacketLenInPacketSplitter() {
        return currentPacketLen;
    }


    public void setCurrentPacketLenInPacketSplitter(int currentPacketLen) {
        this.currentPacketLen = currentPacketLen;
    }


    public void setOffsetInPacketSplitter(int offset) {
        this.offset = offset;
    }


    public int getPacketLenInPacketSplitter() {
        return currentPacketLen;
    }


    public int getOffsetInPacketSplitter() {
        return offset;
    }


    public void onSocketRead(AbstractMySQLSession session) throws IOException {

    }


    public void onWriteFinished(AbstractMySQLSession session) throws IOException {

    }


    public void onSocketClosed(AbstractMySQLSession session, boolean normal) {
        if (!normal) {
            onError(getCurrentMySQLSession().getLastThrowableAndReset());
        } else {
            onWriteFinished(buffer, true);
        }
    }
}
