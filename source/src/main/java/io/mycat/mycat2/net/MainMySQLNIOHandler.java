package io.mycat.mycat2.net;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.proxy.NIOHandler;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

/**
 * 负责MySQLSession的NIO事件 MySQL用户登录阶段通过以后，NIO事件主要由他来响应
 *
 * @author wuzhihui
 */
public class MainMySQLNIOHandler implements NIOHandler<MySQLSession> {
    public static final MainMySQLNIOHandler INSTANCE = new MainMySQLNIOHandler();
    private static Logger logger = LoggerFactory.getLogger(MainMySQLNIOHandler.class);

    public void onSocketRead(final MySQLSession session) throws IOException {
        // 交给SQLComand去处理
        MycatSession mycatSession = session.getMycatSession();
        MySQLCommand curCmd = mycatSession.curSQLCommand;
        try {
            if (curCmd.onBackendResponse(session)) {
                curCmd.clearBackendResouces(session, session.isClosed());
            }
        } catch (ClosedChannelException ex) {
            String errmsg = " read backend response error ,backend conn has closed.";
            logger.error(errmsg);
            session.getMycatSession().closeBackendAndResponseError(session, false, ErrorCode.ERR_CONNECT_SOCKET,
                    errmsg);
        } catch (IOException e) {
            logger.error(" read backend response error.", e);
            session.getMycatSession().closeBackendAndResponseError(session, false, ErrorCode.ERR_CONNECT_SOCKET,
                    e.getMessage());
        }
    }

    /**
     * 前端连接关闭后，延迟关闭会话
     *
     * @param session
     * @param normal
     */
    public void onSocketClosed(MySQLSession session, boolean normal) {
        try {
            session.getMycatSession().curSQLCommand.onBackendClosed(session, normal);
        } catch (IOException e) {
            logger.warn("caught err ", e);
        }
    }

    @Override
    public void onSocketWrite(MySQLSession session) throws IOException {
        session.writeToChannel();

    }

    @Override
    public void onConnect(SelectionKey curKey, MySQLSession session, boolean success, String msg) {
        throw new java.lang.RuntimeException("not implemented ");
    }

    @Override
    public void onWriteFinished(MySQLSession session) throws IOException {
        // 交给SQLComand去处理
        MycatSession mycatSs = session.getMycatSession();
        if (mycatSs.curSQLCommand.onBackendWriteFinished(session)) {
            mycatSs.curSQLCommand.clearBackendResouces(session, false);
        }
    }
}
