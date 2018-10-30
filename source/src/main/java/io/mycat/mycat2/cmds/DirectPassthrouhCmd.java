package io.mycat.mycat2.cmds;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
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
                session.curBackend.startResponse(MySQLSession.ResponseState.COM_QUERY);
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
        boolean check = false;
        while (true) {
            AbstractMySQLSession.CurrPacketType pkgTypeEnum = session.resolveMySQLPackage();
            if (null != pkgTypeEnum && AbstractMySQLSession.CurrPacketType.Full == pkgTypeEnum) {
                //@todo need refactor
                if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {// 如果当前为错误包，则进交给错误包处理
                    check = JudgeUtil.judgeErrorPacket(session, session.proxyBuffer);
                } else if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {// 进行当前执行的预处理的语句报文，如果为Ok则表示可以释放连接,进行正常的判断
                    check = JudgeUtil.judgeOkPacket(session, session.proxyBuffer);
                }
                if (!check) {
                    //@todo
                    session.responseState = MySQLSession.ResponseState.RESULT_SET_SECOND_EOF;
                    //  session.getAttrMap().remove(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG);
                }
            } else {
                break;
            }
        }
        MycatSession mycatSession = session.getMycatSession();
        ProxyBuffer buffer = session.getProxyBuffer();
        buffer.flip();
        mycatSession.takeOwner(SelectionKey.OP_WRITE);
        mycatSession.writeToChannel();
        return false;
    }

    @Override
    public boolean onFrontWriteFinished(MycatSession session) throws IOException {
        // 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
        // 检查当前已经结束，进行切换
        // 检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
        // EnumMap<SessionKeyEnum, Object> sessionAttrMap = session.getAttrMap();
        if (!session.curBackend.isResponseFinished()) {
            session.proxyBuffer.flip();
            session.giveupOwner(SelectionKey.OP_READ);
            return false;
        }
        // 当传输标识不存在，则说已经结束，则切换到前端的读取
        else {
            session.proxyBuffer.flip();
            // session.chnageBothReadOpts();
            session.takeOwner(SelectionKey.OP_READ);
            return true;
        }
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession session) {
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
