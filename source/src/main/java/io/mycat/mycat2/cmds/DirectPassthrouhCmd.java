package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.judge.MySQLProxyStateMHepler;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 直接透传命令报文
 *
 * @author wuzhihui
 */
public class DirectPassthrouhCmd implements MySQLCommand {

    public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();
    private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);

    @Override
    public boolean procssSQL(MycatSession session) throws IOException {
        /*
         * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
         */
        session.clearReadWriteOpts();

        session.getBackend((mysqlsession, sender, success, result) -> {
            ProxyBuffer curBuffer = session.proxyBuffer;
            // 切换 buffer 读写状态
            curBuffer.flip();
            if (success) {
                session.curBackend.responseStateMachine.in(mysqlsession.getMycatSession().getSqltype());
                // 没有读取,直接透传时,需要指定 透传的数据 截止位置
                curBuffer.readIndex = curBuffer.writeIndex;
                // 改变 owner，对端Session获取，并且感兴趣写事件
                session.giveupOwner(SelectionKey.OP_WRITE);
                mysqlsession.writeToChannel();
            } else {
                session.closeBackendAndResponseError(mysqlsession, success, ((ErrorPacket) result));
            }
        });
        return false;
    }

    @Override
    public boolean onBackendResponse(MySQLSession session) throws IOException {
        // 首先进行一次报文的读取操作
        if (!session.readFromChannel()) {
            return false;
        }
        boolean isCommandFinished = false;
        ProxyBuffer curBuffer = session.proxyBuffer;
        while (session.isResolveMySQLPackageFinished()) {
            CurrPacketType pkgTypeEnum = session.resolveCrossBufferMySQLPackage();
            if (CurrPacketType.Full == pkgTypeEnum) {
                isCommandFinished = MySQLProxyStateMHepler.on(session.responseStateMachine, (byte) session.curMSQLPackgInf.pkgType, curBuffer, session);
                session.setIdle(!session.responseStateMachine.isInteractive());
            } else if (CurrPacketType.LongHalfPacket == pkgTypeEnum) {
                isCommandFinished = MySQLProxyStateMHepler.on(session.responseStateMachine, (byte) session.curMSQLPackgInf.pkgType, curBuffer, session);
                session.setIdle(!session.responseStateMachine.isInteractive());
                session.forceCrossBuffer();
                break;
            } else if (CurrPacketType.ShortHalfPacket == pkgTypeEnum ||
                    CurrPacketType.RestCrossBufferPacket == pkgTypeEnum ||
                    CurrPacketType.FinishedCrossBufferPacket == pkgTypeEnum) {
                break;
            }
        }
        MycatSession mycatSession = session.getMycatSession();
        ProxyBuffer buffer = session.getProxyBuffer();
        buffer.flip();
        if (isCommandFinished) {
            logger.debug("mycatSession.takeOwner(SelectionKey.OP_READ)");
            mycatSession.takeOwner(SelectionKey.OP_READ);
        } else {
            logger.debug("mycatSession.takeOwner(SelectionKey.OP_WRITE)");
            mycatSession.takeOwner(SelectionKey.OP_WRITE);
        }
        mycatSession.writeToChannel();
        return false;
    }

    @Override
    public boolean onFrontWriteFinished(MycatSession session) throws IOException {
//         判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
//         检查当前已经结束，进行切换
//         检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
        if (!session.curBackend.responseStateMachine.isFinished()) {
            session.proxyBuffer.flip();
            session.giveupOwner(SelectionKey.OP_READ);
            return false;
        }
        //   当传输标识不存在，则说已经结束，则切换到前端的读取
        else {
            session.proxyBuffer.flip();
            session.takeOwner(SelectionKey.OP_READ);
            return true;
        }
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession session) {
        if (session.responseStateMachine.isFinished()) {
            MycatSession mycatSession = session.getMycatSession();
            mycatSession.proxyBuffer.flip();
            mycatSession.takeOwner(SelectionKey.OP_READ);
            return true;
        }
        // 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
        // 向后端写入完成数据后，则从后端读取数据
        session.proxyBuffer.flip();
        // 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
        session.change2ReadOpts();
        return false;

    }

    @Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) {

        return true;
    }

    @Override
    public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
        if (sessionCLosed) {
            session.recycleAllocedBuffer(session.getProxyBuffer());
            session.unbindAllBackend();
        }
    }

    @Override
    public void clearBackendResouces(MySQLSession mysqlSession, boolean sessionCLosed) {
        if (sessionCLosed) {
            mysqlSession.recycleAllocedBuffer(mysqlSession.getProxyBuffer());
        }
    }
}
