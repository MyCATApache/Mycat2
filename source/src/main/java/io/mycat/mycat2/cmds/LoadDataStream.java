package io.mycat.mycat2.cmds;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public enum LoadDataStream implements NIOHandler<AbstractMySQLSession> {


    CLIENT_2_SERVER_COM_QUERY,
    SERVER_2_CLIENT_COM_QUERY_RESPONSE,
    CLIENT_2_SERVER_CONTENT_FILENAME,
    CLIENT_2_SERVER_EMPTY_PACKET,
    SERVER_2_CLIENT_OK_PACKET;
    private static Logger logger = LoggerFactory.getLogger(LoadDataStream.class);

    /**
     * loaddata传送结束标识长度
     */
    private static final int FLAGLENGTH = 4;

    @Override
    public void onConnect(SelectionKey curKey, AbstractMySQLSession session, boolean success, String msg) throws IOException {
        throw new UnsupportedOperationException();
    }

    void change2(MycatSession session, LoadDataStream loadDataStream) {
        logger.info("from {} to {}", this, loadDataStream);
        session.setCurNIOHandler(loadDataStream);
        session.curBackend.setCurNIOHandler(loadDataStream);
    }

    @Override
    public void onSocketRead(AbstractMySQLSession session) throws IOException {
        switch (this) {

            case SERVER_2_CLIENT_COM_QUERY_RESPONSE:
            case SERVER_2_CLIENT_OK_PACKET: {
                MySQLSession mySQLSession = (MySQLSession) session;
                if (mySQLSession.readFromChannel()) {
                    MycatSession mycatSession = mySQLSession.getMycatSession();
                    CurrPacketType currPacketType = mycatSession.resolveMySQLPackage();
                    if (currPacketType == CurrPacketType.Full) {
                        //不透传
                        ProxyBuffer proxyBuffer = mycatSession.proxyBuffer;
                        proxyBuffer.flip();
                        mycatSession.takeOwner(SelectionKey.OP_WRITE);
                        mycatSession.writeToChannel();
                    }
                }
                break;
            }
            case CLIENT_2_SERVER_CONTENT_FILENAME: {
                MycatSession mycatSession = (MycatSession) session;
                if (mycatSession.readFromChannel()) {
                    readOverByte(mycatSession, mycatSession.proxyBuffer);
                    if (checkOver(mycatSession)) {
                        change2(mycatSession, CLIENT_2_SERVER_EMPTY_PACKET);
                    }
                    mycatSession.proxyBuffer.flip();
                    mycatSession.proxyBuffer.readIndex = mycatSession.proxyBuffer.writeIndex;
                    mycatSession.giveupOwner(SelectionKey.OP_WRITE);
                    mycatSession.curBackend.writeToChannel();
                }
                break;
            }
        }
    }

    @Override
    public void onWriteFinished(AbstractMySQLSession session) throws IOException {
        MycatSession mycatSession =
                (session instanceof MycatSession) ? (MycatSession) session :
                        ((MySQLSession) session).getMycatSession();
        switch (this) {
            case CLIENT_2_SERVER_COM_QUERY:
                mycatSession.proxyBuffer.flip();
                change2(mycatSession, SERVER_2_CLIENT_COM_QUERY_RESPONSE);
                mycatSession.giveupOwner(SelectionKey.OP_READ);
                break;
            case SERVER_2_CLIENT_COM_QUERY_RESPONSE:
                mycatSession.proxyBuffer.flip();
                change2(mycatSession, CLIENT_2_SERVER_CONTENT_FILENAME);
                mycatSession.takeOwner(SelectionKey.OP_READ);
                break;
            case CLIENT_2_SERVER_CONTENT_FILENAME:
                mycatSession.proxyBuffer.flip();
                mycatSession.takeOwner(SelectionKey.OP_READ);
                break;
            case CLIENT_2_SERVER_EMPTY_PACKET:
                mycatSession.proxyBuffer.flip();
                change2(mycatSession, SERVER_2_CLIENT_OK_PACKET);
                mycatSession.giveupOwner(SelectionKey.OP_READ);
                break;
            case SERVER_2_CLIENT_OK_PACKET:
                mycatSession.proxyBuffer.flip();
                mycatSession.takeOwner(SelectionKey.OP_READ);
                mycatSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
                mycatSession.curBackend.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
                break;
        }

    }


    @Override
    public void onSocketWrite(AbstractMySQLSession session) throws IOException {
        session.writeToChannel();
    }

    @Override
    public void onSocketClosed(AbstractMySQLSession session, boolean normal) {

    }

    /**
     * 进行结束符的读取
     *
     * @param curBuffer buffer数组信息
     */
    private void readOverByte(MycatSession session, ProxyBuffer curBuffer) {
        byte[] overFlag = getOverFlag(session);
        // 获取当前buffer的最后
        ByteBuffer buffer = curBuffer.getBuffer();

        // 如果数据的长度超过了，结束符的长度，可直接提取结束符
        if (buffer.position() >= FLAGLENGTH) {
            int opts = curBuffer.writeIndex;
            buffer.position(opts - FLAGLENGTH);
            buffer.get(overFlag, 0, FLAGLENGTH);
            buffer.position(opts);
        }
        // 如果小于结束符，说明需要进行两个byte数组的合并
        else {
            int opts = curBuffer.writeIndex;
            // 计算放入的位置
            int moveSize = FLAGLENGTH - opts;
            int index = 0;
            // 进行数组的移动,以让出空间进行放入新的数据
            for (int i = FLAGLENGTH - moveSize; i < FLAGLENGTH; i++) {
                overFlag[index] = overFlag[i];
                index++;
            }
            // 读取数据
            buffer.position(0);
            buffer.get(overFlag, moveSize, opts);
            buffer.position(opts);
        }

    }

    /**
     * 进行结束符的检查,
     * <p>
     * 数据的结束符为0,0,0,包序，即可以验证读取到3个连续0，即为结束
     *
     * @return
     */
    private boolean checkOver(MycatSession session) {
        byte[] overFlag = getOverFlag(session);
        for (int i = 0; i < overFlag.length - 1; i++) {
            if (overFlag[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /*获取结束flag标识的数组*/
    private byte[] getOverFlag(MycatSession session) {
        byte[] overFlag = (byte[]) session.getAttrMap().get(SessionKey.LOAD_OVER_FLAG_ARRAY);
        if (overFlag != null) {
            return overFlag;
        }
        overFlag = new byte[FLAGLENGTH];
        session.getAttrMap().put(SessionKey.LOAD_OVER_FLAG_ARRAY, overFlag);
        return overFlag;
    }
}
