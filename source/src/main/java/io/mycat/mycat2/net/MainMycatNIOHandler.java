package io.mycat.mycat2.net;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.ComQuitCmd;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.LoadDataCommand;
import io.mycat.mycat2.cmds.manager.MyCatCmdDispatcher;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.proxy.NIOHandler;
import io.mycat.util.ErrorCode;
import io.mycat.util.LoadDataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static io.mycat.mycat2.cmds.LoadDataState.CLIENT_2_SERVER_EMPTY_PACKET;
import static io.mycat.mysql.MySQLPacketInf.resolveFullPayloadExpendBuffer;

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
        if (!readed)
            return;
        MySQLCommand curCmd = session.getCurSQLCommand();
        if (curCmd == null) {
            MySQLPacketInf packetInf = session.curPacketInf;
            session.curPacketInf.proxyBuffer = session.proxyBuffer;
            if (!resolveFullPayloadExpendBuffer(session)) {
                return;
            }
            try {
                processSQL(session);
            } catch (RuntimeException e){
                throw e;
            } finally {
                packetInf.recycleLargePayloadBuffer();
            }
        } else {
            //当前的SQLCommand没有处理完请求，继续处理
            if (curCmd.procssSQL(session)) {
                curCmd.clearResouces(session, session.isClosed());
                session.switchSQLCommand(null);
            }
        }
    }

    private void processSQL(final MycatSession session) throws IOException {
        switch (session.curPacketInf.head) {
            case MySQLCommand.COM_QUERY: {
                doQuery(session);
                return;
            }
            case MySQLCommand.COM_QUIT: {
                session.switchSQLCommand(ComQuitCmd.INSTANCE);
                break;
            }
            default: {
                session.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
                break;
            }
        }
        if (session.getCurSQLCommand().procssSQL(session)) {
            session.getCurSQLCommand().clearResouces(session, session.isClosed());
            session.switchSQLCommand(null);
        }
        // if (!delegateRoute(session)) {
        // return false;
        // }

        // /**
        // * 设置原始处理命令
        // * 1. 设置目标命令
        // * 2. 处理动态注解
        // * 3. 处理静态注解
        // * 4. 构建命令或者注解链。 如果没有注解链，直接返回目标命令
        // */
        // SQLAnnotationChain chain = new SQLAnnotationChain();
        // session.curSQLCommand =
        // chain.setTarget(command).processDynamicAnno(session)
        // .processStaticAnno(session, staticAnnontationMap).build();
    }

    private void doQuery(final MycatSession session) throws IOException {
        MySQLCommand command;
        try {
            int startIndex = session.curPacketInf.largePayload.position();
            ByteBuffer duplicate = session.curPacketInf.largePayload.duplicate();
            byte[] a = new byte[duplicate.limit() - duplicate.position()];
            duplicate.get(a);
            System.out.println(new String(a));
            int endIndex = session.curPacketInf.largePayload.limit();
            session.parser.parse(session.curPacketInf.largePayload, startIndex, endIndex - startIndex, session.sqlContext);
        } catch (Exception e) {
            try {
                logger.error("sql parse error", e);
                session.sendErrorMsg(ErrorCode.ER_PARSE_ERROR, "sql parse error : " + e.getMessage());
            } catch (Exception e1) {
                session.close(false, e1.getMessage());
            }
            return;
        }
        byte sqltype = session.sqlContext.getSQLType() != 0 ? session.sqlContext.getSQLType()
                : session.sqlContext.getCurSQLType();
        session.setSqltype(sqltype);
        switch (sqltype) {
            case BufferSQLContext.MYCAT_SQL:
                command = MyCatCmdDispatcher.INSTANCE.getMycatCommand(session.sqlContext);
                break;
            case BufferSQLContext.ANNOTATION_SQL:
                command = MyCatCmdDispatcher.INSTANCE.getMycatCommand(session.sqlContext);
                break;
            case BufferSQLContext.SET_AUTOCOMMIT_SQL:
            case BufferSQLContext.START_TRANSACTION_SQL:
            case BufferSQLContext.XA_BEGIN:
                logger.debug("received transaction sql,type {}", sqltype);
                // @todo transaction status ??
                command = DirectPassthrouhCmd.INSTANCE;
                break;
            case BufferSQLContext.LOAD_SQL:
                command = LoadDataCommand.INSTANCE;
                break;
            default:
                command = DirectPassthrouhCmd.INSTANCE;
                break;
        }
        session.switchSQLCommand(command);
        if (command.procssSQL(session)) {
            command.clearResouces(session, session.isClosed());
            session.switchSQLCommand(null);
        }
    }

    private void resolveHalfPacket(MycatSession session) throws IOException {
        int pkgLength = session.curPacketInf.pkgLength;
        ByteBuffer bytebuffer = session.proxyBuffer.getBuffer();
        if (pkgLength > bytebuffer.capacity() && !bytebuffer.hasRemaining()) {
            try {
                session.ensureFreeSpaceOfReadBuffer();
            } catch (RuntimeException e1) {
                session.sendErrorMsg(ErrorCode.ER_UNKNOWN_ERROR, e1.getMessage());
            }
        }
    }

    private void resolveLoadData(final MycatSession mycatSession) throws IOException {
        LoadDataUtil.readOverByte(mycatSession, mycatSession.proxyBuffer);
        if (LoadDataUtil.checkOver(mycatSession)) {
            LoadDataUtil.change2(mycatSession, CLIENT_2_SERVER_EMPTY_PACKET);
        }
        mycatSession.proxyBuffer.flip();
        mycatSession.proxyBuffer.readIndex = mycatSession.proxyBuffer.writeIndex;
        mycatSession.giveupOwner(SelectionKey.OP_WRITE);
        mycatSession.getCurBackend().writeToChannel();
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
        logger.debug("write finished  {}", this);
        MySQLCommand curCmd = session.getCurSQLCommand();
        if (curCmd != null) {
            // 交给SQLComand去处理
            if (curCmd.onFrontWriteFinished(session)) {
                curCmd.clearResouces(session, false);
                session.switchSQLCommand(null);
            }
        }

    }

}
