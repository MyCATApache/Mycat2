package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MycatException;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mysql.MultiPacketWriter;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.util.List;

import static io.mycat.mysql.MySQLPacketInf.directPassthrouhBuffer;

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

        MySQLSession curBackend = session.getCurBackend();
        List<ProxyBuffer> multiPackets = session.curPacketInf.payloadReader.getMultiPackets();
        session.curPacketInf.multiPacketWriter.init(multiPackets);
        if (curBackend != null) {//@todo,需要检测后端session是否有效
            if (session.getTargetDataNode() == null) {
                logger.warn("{} not specified SQL target DataNode ,so set to default dataNode ", session);
                DNBean targetDataNode = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap()
                        .get(session.getMycatSchema().getDefaultDataNode());
                session.setTargetDataNode(targetDataNode);
            }
            if (curBackend.synchronizedState(session.getTargetDataNode().getDatabase()) && curBackend.isActivated()) {
                if (!curBackend.isIdle()) {
                    throw new RuntimeException("Illegal state");
                }
                this.firstWriteToChannel(session,session.curPacketInf.multiPacketWriter);
            } else {
                // 同步数据库连接状态后回调
                AsynTaskCallBack<MySQLSession> callback = (mysqlsession, sender, success, result) -> {
                    if (success) {
                        firstWriteToChannel(session,session.curPacketInf.multiPacketWriter);
                    } else {
                        session.closeAllBackendsAndResponseError(success, ((ErrorPacket) result));
                    }
                };
                curBackend.syncAndCallback(callback);
            }
        } else {// 没有当前连接，尝试获取新连接
            session.getBackendAndCallBack((mysqlsession, sender, success, result) -> {
                if (success) {
                    firstWriteToChannel(session,session.curPacketInf.multiPacketWriter);
                } else {
                    session.closeAllBackendsAndResponseError(success, ((ErrorPacket) result));
                }
            });
        }

        return false;
    }

    private void firstWriteToChannel(MycatSession session, Iterator<ProxyBuffer> writer) throws IOException {
        session.clearReadWriteOpts();
        MySQLSession curBackend = session.getCurBackend();
        Iterator<ProxyBuffer> multiPacketWriter = writer;
        if (multiPacketWriter.hasNext()) {
            session.proxyBuffer = session.curPacketInf.proxyBuffer= multiPacketWriter.next();
            System.out.println(StringUtil.dumpAsHex(session.proxyBuffer.getBuffer()));
            session.giveupOwner(SelectionKey.OP_WRITE);
            curBackend.writeToChannel();
        } else {
            throw new MycatException("不可能没有数据");
        }
    }

    private boolean continueWriteToChannel(MycatSession session, Iterator<ProxyBuffer> writer) throws IOException {
        Iterator<ProxyBuffer> multiPacketWriter = writer;
        MySQLSession curBackend = session.getCurBackend();
        boolean b = multiPacketWriter.hasNext();
        if (b) {
            session.bufPool.recycle(session.proxyBuffer.getBuffer());
            session.proxyBuffer = session.curPacketInf.proxyBuffer= multiPacketWriter.next();
            curBackend.writeToChannel();
        } else {
            // 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
            // 向后端写入完成数据后，则从后端读取数据
            curBackend.proxyBuffer.reset();
            curBackend.proxyBuffer.flip();
            // 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
            curBackend.change2ReadOpts();
        }
        return b;
    }
    @Override
    public boolean onBackendResponse(MySQLSession mySQLSession) throws IOException {
        // 首先进行一次报文的读取操作
        if (!mySQLSession.readFromChannel()) {
            return false;
        }
        MySQLPacketInf packetInf = directPassthrouhBuffer(mySQLSession);
        MycatSession mycatSession = mySQLSession.getMycatSession();
        ProxyBuffer buffer = mySQLSession.getProxyBuffer();
        buffer.flip();
        if (packetInf.isResponseFinished()) {
            mycatSession.takeOwner(SelectionKey.OP_READ);
            mySQLSession.setIdle(!packetInf.isInteractive());
        } else {
            mycatSession.takeOwner(SelectionKey.OP_WRITE);
        }
        mycatSession.writeToChannel();
        return false;
    }

    @Override
    public boolean onFrontWriteFinished(MycatSession mycatSession) throws IOException {
        // 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
        // 检查当前已经结束，进行切换
        // 检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
        MySQLSession mySQLSession = mycatSession.getCurBackend();

        if (mySQLSession != null && mySQLSession.curPacketInf != null && !mySQLSession.curPacketInf.isResponseFinished()) {
            mycatSession.proxyBuffer.flip();
            mycatSession.giveupOwner(SelectionKey.OP_READ);
            return false;
        }
        // 当传输标识不存在，则说已经结束，则切换到前端的读取
        else {
            mycatSession.curPacketInf.payloadReader.clear();
            mycatSession.proxyBuffer.flip();
            mycatSession.takeOwner(SelectionKey.OP_READ);
            mycatSession.proxyBuffer.reset();
            return true;
        }
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession mySQLSession) throws IOException {
        MycatSession mycatSession = mySQLSession.getMycatSession();
        MySQLPacketInf mycatPacketInf = mycatSession.curPacketInf;
        if (mycatPacketInf.needContinueOnReadingRequest()) {
            mycatSession.proxyBuffer.flip();
            mycatSession.takeOwner(SelectionKey.OP_READ);
            return true;
        }

        return continueWriteToChannel(mycatSession,mycatSession.curPacketInf.multiPacketWriter);

    }

    @Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) {
        return true;
    }

    @Override
    public void clearResouces(MycatSession session, boolean sessionCLosed) {
        if (sessionCLosed) {
            session.recycleAllocedBuffer(session.getProxyBuffer());
            session.unbindBackends();
        }
    }

}
