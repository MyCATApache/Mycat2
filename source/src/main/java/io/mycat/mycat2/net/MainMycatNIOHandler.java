package io.mycat.mycat2.net;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.LoadDataState;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;
import io.mycat.util.LoadDataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static io.mycat.mycat2.cmds.LoadDataState.CLIENT_2_SERVER_EMPTY_PACKET;

/**
 * 负责MycatSession的NIO事件，驱动SQLCommand命令执行，完成SQL的处理过程
 * Mycat用户登录阶段通过以后，NIO事件主要由他来响应，包括派发到具体的SQLCommand。
 *
 * @author wuzhihui
 */
public class MainMycatNIOHandler implements NIOHandler<MycatSession> {
    public static final MainMycatNIOHandler INSTANCE = new MainMycatNIOHandler();
    private static Logger logger = LoggerFactory.getLogger(MainMycatNIOHandler.class);

    public void onSocketRead(final MycatSession session) throws IOException {
        boolean readed = session.readFromChannel();
        if (!readed) return;
        if (session.loadDataStateMachine == LoadDataState.CLIENT_2_SERVER_CONTENT_FILENAME) {
            resolveLoadData(session);
            return;
        } else {
            CurrPacketType currPacketType = session.resolveMySQLPackage(false, false);
            if (CurrPacketType.Full == currPacketType) {
                session.changeToDirectIfNeed();
            } else if (CurrPacketType.LongHalfPacket == currPacketType || CurrPacketType.ShortHalfPacket == currPacketType) {
                if (!resolveHalfPackage(session)) return;
                session.proxyBuffer.readMark = session.proxyBuffer.readIndex;
                return;
            }
        }
        ProxyBuffer buffer = session.getProxyBuffer();
        if (session.curMSQLPackgInf.endPos < buffer.writeIndex) {
            logger.warn("front contains multi package ");
        }
        if (!session.matchMySqlCommand()) {
            return;
        }
        // 如果当前包需要处理，则交给对应方法处理，否则直接透传
        if (session.curSQLCommand.procssSQL(session)) {
            session.curSQLCommand.clearFrontResouces(session, session.isClosed());
        }
    }

    private boolean resolveHalfPackage(MycatSession session) throws IOException {
        int pkgLength = session.curMSQLPackgInf.pkgLength;
        ByteBuffer bytebuffer = session.proxyBuffer.getBuffer();
        if (pkgLength > bytebuffer.capacity() && !bytebuffer.hasRemaining()) {
            try {
                session.ensureFreeSpaceOfReadBuffer();
            } catch (RuntimeException e1) {
                if (!session.curMSQLPackgInf.crossBuffer) {
                    session.curMSQLPackgInf.crossBuffer = true;
                    session.curMSQLPackgInf.remainsBytes = pkgLength
                            - (session.curMSQLPackgInf.endPos - session.curMSQLPackgInf.startPos);
                    session.sendErrorMsg(ErrorCode.ER_UNKNOWN_ERROR, e1.getMessage());
                }
                session.proxyBuffer.readIndex = session.proxyBuffer.writeIndex;
                return false;
            }
        }
        return true;
    }

    private void resolveLoadData(final MycatSession mycatSession) throws IOException {
        LoadDataUtil.readOverByte(mycatSession, mycatSession.proxyBuffer);
        if (LoadDataUtil.checkOver(mycatSession)) {
            LoadDataUtil.change2(mycatSession, CLIENT_2_SERVER_EMPTY_PACKET);
        }
        mycatSession.proxyBuffer.flip();
        mycatSession.proxyBuffer.readIndex = mycatSession.proxyBuffer.writeIndex;
        mycatSession.giveupOwner(SelectionKey.OP_WRITE);
        mycatSession.curBackend.writeToChannel();
    }

    /**
     * 前端连接关闭后，延迟关闭会话
     *
     * @param session
     * @param normal
     */
    public void onSocketClosed(MycatSession session, boolean normal) {
        logger.info("front socket closed " + session);
        session.lazyCloseSession(normal, "front closed");
    }

    @Override
    public void onSocketWrite(MycatSession session) throws IOException {
        session.writeToChannel();

    }

    @Override
    public void onConnect(SelectionKey curKey, MycatSession session, boolean success, String msg) {
        throw new java.lang.RuntimeException("not implemented ");
    }

    @Override
    public void onWriteFinished(MycatSession session) throws IOException {
        // 交给SQLComand去处理
        if (session.curSQLCommand.onFrontWriteFinished(session)) {
            session.curSQLCommand.clearFrontResouces(session, false);
        }
    }

}
