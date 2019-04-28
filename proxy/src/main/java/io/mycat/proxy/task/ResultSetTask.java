package io.mycat.proxy.task;

import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketCallback;
import io.mycat.proxy.packet.MySQLPacketProcessType;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.session.MySQLSession;

import java.io.IOException;

public interface ResultSetTask extends NIOHandler<MySQLSession>, MySQLPacketCallback {
    default public void request(MySQLSession mysql, int head, String data, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, head, data, (MycatReactorThread) Thread.currentThread(), callBack);
    }

    default public void requestEmptyPacket(MySQLSession mysql, byte nextPacketId, AsynTaskCallBack<MySQLSession> callBack) {
        requestEmptyPacket(mysql, nextPacketId, (MycatReactorThread) Thread.currentThread(), callBack);
    }

    default public void requestEmptyPacket(MySQLSession mysql, byte nextPacketId, MycatReactorThread curThread, AsynTaskCallBack<MySQLSession> callBack) {
        try {
            mysql.setCallBack(callBack);
            mysql.switchNioHandler(this);
            if (mysql.currentProxyBuffer() != null) {
                throw new MycatExpection("");
            }
            mysql.setProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
            MySQLPacket mySQLPacket = mysql.newCurrentMySQLPacket();
            mysql.prepareReveiceResponse();
            mysql.writeMySQLPacket(mySQLPacket, nextPacketId);
        } catch (IOException e) {
            this.clearAndFinished(false, e.getMessage());
        }
    }

    default public void request(MySQLSession mysql, int head, byte[] data, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, head, data, (MycatReactorThread) Thread.currentThread(), callBack);
    }

    default public void request(MySQLSession mysql, int head, long data, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, head, data, (MycatReactorThread) Thread.currentThread(), callBack);
    }

    default public void request(MySQLSession mysql, int head, String data, MycatReactorThread curThread, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, head, data.getBytes(), curThread, callBack);
    }
    default public void request(MySQLSession mysql, int head, byte[] data, MycatReactorThread curThread, AsynTaskCallBack<MySQLSession> callBack) {
        try {
            mysql.setCallBack(callBack);
            mysql.switchNioHandler(this);
            if (mysql.currentProxyBuffer() != null) {
//                throw new MycatExpection("");
                mysql.currentProxyBuffer().reset();
            }
            mysql.setProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
            MySQLPacket mySQLPacket = mysql.newCurrentMySQLPacket();
            mySQLPacket.writeByte((byte) head);
            mySQLPacket.writeBytes(data);
            mysql.prepareReveiceResponse();
            mysql.writeMySQLPacket(mySQLPacket, mysql.setPacketId(0));
        } catch (IOException e) {
            this.clearAndFinished(false, e.getMessage());
        }
    }

    default public void request(MySQLSession mysql, int head, long data, MycatReactorThread curThread, AsynTaskCallBack<MySQLSession> callBack) {
        try {
            mysql.setCallBack(callBack);
            mysql.switchNioHandler(this);
            if (mysql.currentProxyBuffer() != null) {
                throw new MycatExpection("");
            }
            mysql.setProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
            MySQLPacket mySQLPacket = mysql.newCurrentMySQLPacket();
            mySQLPacket.writeByte((byte) head);
            mySQLPacket.writeFixInt(4, data);
            mysql.prepareReveiceResponse();
            mysql.writeMySQLPacket(mySQLPacket, 0);
        } catch (IOException e) {
            this.clearAndFinished(false, e.getMessage());
        }
    }

    @Override
    default void onFinished(boolean success, String errorMessage) {
        MySQLSession currentMySQLSession = getCurrentMySQLSession();
        AsynTaskCallBack<MySQLSession> callBack = currentMySQLSession.getCallBackAndReset();
        callBack.finished(currentMySQLSession, this, success, getResult(), errorMessage);
    }

    default Object getResult() {
        return null;
    }

    @Override
    default public void onSocketRead(MySQLSession mysql) throws IOException {
        try {
            if (mysql.getCurNIOHandler() != this) {
                return;
            }
            if (!mysql.readFromChannel()) {
                return;
            }
            MySQLPacketResolver resolver = mysql.getPacketResolver();
            ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
            proxyBuffer.newBufferIfNeed();
            MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
            boolean isResponseFinished = false;
            while (mysql.getCurNIOHandler() == this && mysql.readMySQLPayloadFully()) {
                MySQLPacketProcessType type = mysql.getPacketResolver().getMySQLPacketProcessType();
                isResponseFinished = mysql.isResponseFinished();
                int startPos = resolver.getStartPos();
                int endPos = resolver.getEndPos();
                switch (type) {
                    case REQUEST:
                        this.onRequest(mySQLPacket, startPos, endPos);
                        break;
                    case LOAD_DATA_REQUEST:
                        this.onLoadDataRequest(mySQLPacket, startPos, endPos);
                        break;
                    case REQUEST_SEND_LONG_DATA:
                        this.onPrepareLongData(mySQLPacket, startPos, endPos);
                        break;
                    case REQUEST_COM_STMT_CLOSE:
                        break;
                    case FIRST_ERROR:
                        this.onError(mySQLPacket, startPos, endPos);
                        break;
                    case FIRST_OK:
                        this.onOk(mySQLPacket, startPos, endPos);
                        break;
                    case FIRST_EOF:
                        this.onEof(mySQLPacket, startPos, endPos);
                        break;
                    case COLUMN_COUNT:
                        this.onColumnCount(resolver.getColumnCount());
                        break;
                    case COLUMN_DEF:
                        this.onColumnDef(mySQLPacket, startPos, endPos);
                        break;
                    case COLUMN_EOF:
                        this.onColumnDefEof(mySQLPacket, startPos, endPos);
                        break;
                    case TEXT_ROW:
                        this.onTextRow(mySQLPacket, startPos, endPos);
                        break;
                    case BINARY_ROW:
                        this.onBinaryRow(mySQLPacket, startPos, endPos);
                        break;
                    case ROW_EOF:
                        this.onRowEof(mySQLPacket, startPos, endPos);
                        break;
                    case ROW_FINISHED:
                        break;
                    case ROW_OK:
                        this.onRowOk(mySQLPacket, startPos, endPos);
                        break;
                    case ROW_ERROR:
                        this.onRowError(mySQLPacket, startPos, endPos);
                        break;
                    case PREPARE_OK:
                        this.onPrepareOk(resolver);
                        break;
                    case PREPARE_OK_PARAMER_DEF:
                        this.onPrepareOkParameterDef(mySQLPacket, startPos, endPos);
                        break;
                    case PREPARE_OK_COLUMN_DEF:
                        this.onPrepareOkColumnDef(mySQLPacket, startPos, endPos);
                        break;
                    case PREPARE_OK_COLUMN_DEF_EOF:
                        this.onPrepareOkColumnDefEof(resolver);
                        break;
                    case PREPARE_OK_PARAMER_DEF_EOF:
                        this.onPrepareOkColumnDefEof(resolver);
                        break;
                }
                if (isResponseFinished) {
                    break;
                } else if (mysql.getCurNIOHandler() == this) {
                    MySQLPacketResolver packetResolver = mysql.getPacketResolver();
                    mySQLPacket.packetReadStartIndex(packetResolver.getEndPos());
                }
            }
            if (isResponseFinished) {
                clearAndFinished(true, null);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            clearAndFinished(false, e.getMessage());
        }
    }

    default public void clearAndFinished(boolean success, String errorMessage) {

        MySQLSession mysql = getCurrentMySQLSession();
        mysql.resetPacket();
        mysql.setProxyBuffer(null);
        mysql.switchDefaultNioHandler();
        onFinished(success, errorMessage);
    }

    @Override
    default public void onWriteFinished(MySQLSession mysql) throws IOException {
        mysql.change2ReadOpts();
    }

    @Override
    default public void onSocketClosed(MySQLSession session, boolean normal) {
        onFinished(false, "socket closed");
    }

}
