package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.StringUtil;
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
                session.curBackend.responseStateMachine.reset(mysqlsession.getMycatSession().getSqltype());
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
        // 获取当前是否结束标识
        boolean proceed = true;
        boolean isCommandFinished = false;
        MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;
        ProxyBuffer curBuffer = session.proxyBuffer;
        while (proceed) {
            CurrPacketType pkgTypeEnum = session.resolveMySQLPackage();
            if (CurrPacketType.Full == pkgTypeEnum) {
                final String hexs = StringUtil.dumpAsHex(session.proxyBuffer.getBuffer(), session.curMSQLPackgInf.startPos, session.curMSQLPackgInf.pkgLength);
                logger.info(session.curMSQLPackgInf.pkgType+"");
                logger.info(hexs);
                isCommandFinished = session.responseStateMachine.on((byte) session.curMSQLPackgInf.pkgType, curBuffer, session);
            } else if (CurrPacketType.LongHalfPacket == pkgTypeEnum) {
//                if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET ||
//                        session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET ||
//                        session.curMSQLPackgInf.pkgType == MySQLPacket.EOF_PACKET) {
//                    // 读取到了EOF/OK/ERROR 类型长半包 是需要保证是整包的.
//                    break;
//                }
//                if (curMSQLPackgInf.crossBuffer) {
//                    // 发生过透传的半包,往往包的长度超过了buffer 的长度.
//                    logger.debug(" readed crossBuffer LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
//                } else {
//                    // 不需要整包解析的长半包透传. result set .这种半包直接透传
//                    curMSQLPackgInf.crossBuffer = true;
//                    curBuffer.readIndex = curMSQLPackgInf.endPos;
//                    curMSQLPackgInf.remainsBytes = curMSQLPackgInf.pkgLength
//                            - (curMSQLPackgInf.endPos - curMSQLPackgInf.startPos);
//                    logger.debug(" readed LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
//                    logger.debug(" curBuffer {}", curBuffer);
//                }
                break;
            } else if (CurrPacketType.ShortHalfPacket == pkgTypeEnum) {
                break;
            }
            proceed = session.proxyBuffer.readIndex != session.proxyBuffer.writeIndex;

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
        if (session.responseStateMachine.isFinished()){
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
