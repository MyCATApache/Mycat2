package io.mycat.proxy.task;

import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.session.AbstractMySQLSession;
import io.mycat.proxy.session.MySQLSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class AbstractByteBufferPayloadWriter<T extends ByteBuffer> implements NIOHandler<AbstractMySQLSession>, PacketSplitter {
    private ByteBuffer[] buffers;
    private int startIndex;
    private int writeIndex;
    private int length;
    BufferPool bufferPool;
    SocketChannel socketChannel;
    int reminsPacketLen;
    int totalSize;
    int currentPacketLen;
    int offset;
    private MySQLSession mysql;

    public void request(MySQLSession mysql, T buffer, int position, int length, AsynTaskCallBack<MySQLSession> callBack) {
        try {
            this.mysql = mysql;
            this.buffers = new ByteBuffer[2];
            this.buffers[1] = buffer;
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
            ByteBuffer header = buffers[0] = getBufferPool().allocate(4);
            boolean hasNext = false;
            while (hasNext = nextPacketInPacketSplitter()) {
                this.writeIndex = startIndex + getOffsetInPacketSplitter();
                setReminsPacketLen(getPacketLenInPacketSplitter());
                setPacketId((byte) (1 + getPacketId()));
                writeHeader(header, getPacketLenInPacketSplitter(), getPacketId());
                int writed = writePayload(buffers, this.writeIndex, getReminsPacketLen(), getServerSocket());
                this.writeIndex += writed;
                setReminsPacketLen(getReminsPacketLen() - writed);
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
            e.printStackTrace();
            onError(e);
        }
    }

    @Override
    public void onSocketWrite(AbstractMySQLSession session) throws IOException {
        int writed = writePayload(buffers, this.writeIndex, getReminsPacketLen(), getServerSocket());
        this.writeIndex += writed;
        setReminsPacketLen(getReminsPacketLen() - writed);
        while (getReminsPacketLen() == 0) {
            if (nextPacketInPacketSplitter()) {
                this.writeIndex = this.startIndex + getOffsetInPacketSplitter();
                setReminsPacketLen(getPacketLenInPacketSplitter());
                setPacketId((byte) (1 + getPacketId()));
                writed = writePayload(buffers, this.writeIndex, getReminsPacketLen(), getServerSocket());
                this.writeIndex += writed;
                setReminsPacketLen(getReminsPacketLen() - writed);
            } else {
                writeFinishedAndClear(true);
                return;
            }
        }
    }

    protected int writePayload(ByteBuffer[] buffer, int writeIndex, int reminsPacketLen, SocketChannel serverSocket) throws IOException {
        ByteBuffer body = buffer[1];
        body.position(writeIndex).limit(reminsPacketLen + writeIndex);
        ByteBuffer header = buffer[0];
        if (header.hasRemaining()) {
            serverSocket.write(buffer);
            return body.position() - writeIndex;
        } else {
            return serverSocket.write(body);
        }
    }
    public void writeFinishedAndClear(boolean success) {
        mysql.clearReadWriteOpts();
        getBufferPool().recycle(buffers[0]);
        buffers[0] = null;
        setServerSocket(null);
        ByteBuffer buffer = buffers[1];
        onWriteFinished((T) buffer, success);
    }

    <T extends ByteBuffer> void writeHeader(T buffer, int packetLen, int packerId) {
        buffer.position(0);
        MySQLPacket.writeFixIntByteBuffer(buffer, 3, packetLen);
        buffer.put((byte) packerId);
        buffer.position(0);
    }

    void onWriteFinished(T buffer, boolean success) {
        AsynTaskCallBack callBackAndReset = getCurrentMySQLSession().getCallBackAndReset();
        try {
            clearResource(buffer);
            callBackAndReset.finished(this.mysql, this, success, null, null);
        } catch (Exception e) {
            mysql.setLastThrowable(e);
            callBackAndReset.finished(this.mysql, this, false, null, null);
        }
    }

    abstract void clearResource(T f) throws Exception;

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



    public void onSocketClosed(MySQLSession session, boolean normal) {
        if (!normal) {
            onError(getCurrentMySQLSession().getLastThrowableAndReset());
        } else {
            writeFinishedAndClear(false);
        }
    }

    @Override
    public void onSocketRead(AbstractMySQLSession session) throws IOException {

    }

    @Override
    public void onWriteFinished(AbstractMySQLSession session) throws IOException {

    }

    @Override
    public void onSocketClosed(AbstractMySQLSession session, boolean normal) {

    }
}
