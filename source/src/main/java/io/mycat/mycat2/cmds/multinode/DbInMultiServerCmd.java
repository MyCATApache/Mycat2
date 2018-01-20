package io.mycat.mycat2.cmds.multinode;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.locks.ReentrantLock;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.AbstractMultiDNExeCmd;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 
 * <b><code>DbInMultiServerCmd</code></b>
 * <p>
 * DbInMultiServer模式下的多节点执行Command类
 * </p>
 * <b>Creation Time:</b> 2018-01-20
 * 
 * @author <a href="mailto:flysqrlboy@gmail.com">zhangsiwei</a>
 * @since 2.0
 */
public class DbInMultiServerCmd extends AbstractMultiDNExeCmd {

    public static final DbInMultiServerCmd INSTANCE = new DbInMultiServerCmd();

    private int backendWritedCount = 0, executeCount = 0;

    private boolean isFirst = true;

    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public boolean procssSQL(MycatSession session) throws IOException {
        RouteResultsetNode[] nodes = session.curRouteResultset.getNodes();
        for (int i = 0; i < nodes.length; i++) {
            RouteResultsetNode node = nodes[i];
            session.curRouteResultsetNode = node;
            /*
             * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
             */
            session.clearReadWriteOpts();

            session.getBackend((mysqlsession, sender, success, result) -> {

                ProxyBuffer curBuffer = session.proxyBuffer;
                // 切换 buffer 读写状态
                curBuffer.flip();

                if (success) {
                    // 没有读取,直接透传时,需要指定 透传的数据 截止位置
                    curBuffer.readIndex = curBuffer.writeIndex;
                    // 改变 owner，对端Session获取，并且感兴趣写事件
                    session.giveupOwner(SelectionKey.OP_WRITE);
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        // 因为第一次把报文透传到mysql后端后，readmark指针会来到readIndex的位置，
                        // 所以第一次之后再要透传同样的指令，就要先把readmark重置回原来的位置。
                        curBuffer.readMark =
                                curBuffer.readIndex - session.curMSQLPackgInf.pkgLength;
                    }
                    try {
                        mysqlsession.writeToChannel();
                    } catch (IOException e) {
                        session.closeBackendAndResponseError(mysqlsession, success,
                                ((ErrorPacket) result));
                    }
                } else {
                    session.closeBackendAndResponseError(mysqlsession, success,
                            ((ErrorPacket) result));
                }
            });
        }
        return false;
    }

    @Override
    public boolean onBackendResponse(MySQLSession session) throws IOException {
        lock.lock();
        try {
            ++executeCount;
            // 首先进行一次报文的读取操作
            if (!session.readFromChannel()) {
                return false;
            }
            // 进行报文处理的流程化
            boolean nextReadFlag = false;
            do {
                // 进行报文的处理流程
                nextReadFlag = session.getMycatSession().commandHandler.procss(session);
            } while (nextReadFlag);

            // 获取当前是否结束标识
            Boolean check = (Boolean) session.getSessionAttrMap()
                    .get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());

            MycatSession mycatSession = session.getMycatSession();
            ProxyBuffer buffer = session.getProxyBuffer();

            if (executeCount < session.getMycatSession().curRouteResultset.getNodes().length) {
                // DbInMultiServer模式下，不考虑show tables等DSL语句的话，只有对全局表的操作才会跨节点，也就是对全局表的DDL，DML语句，
                // 而对每个节点的全局表操作完后返回的报文都是一样的，因此只需要拿最后一次的报文返回给客户端即可
                if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
                    // 因为不是最后一个节点的返回报文，所以这里讲readmark设为readIndex，也就是丢弃掉这次报文（仅考虑全局表的DDL， DML返回报文）
                    // TODO show tables类的DSL语句就不适用，这个后续考虑时再优化
                    session.getProxyBuffer().readMark = session.getProxyBuffer().readIndex;
                }
                return false;
            }

            // 检查到当前已经完成,执行添加操作
            if (null != check && check) {
                // 当知道操作完成后，前段的注册感兴趣事件为读取
                mycatSession.takeOwner(SelectionKey.OP_READ);
            }
            // 未完成执行继续读取操作
            else {
                // 直接透传报文
                mycatSession.takeOwner(SelectionKey.OP_WRITE);
            }
            buffer.flip();
            executeCount = 0;
            mycatSession.writeToChannel();
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
        // TODO Auto-generated method stub
        return super.onBackendClosed(session, normal);
    }

    @Override
    public boolean onFrontWriteFinished(MycatSession session) throws IOException {
        // 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
        // 检查当前已经结束，进行切换
        // 检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
        if (session.getSessionAttrMap()
                .containsKey(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey())) {
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
    public boolean onBackendWriteFinished(MySQLSession session) throws IOException {

        ++backendWritedCount;
        session.proxyBuffer.flip();
        session.change2ReadOpts();
        if (backendWritedCount >= session.getMycatSession().curRouteResultset.getNodes().length) {
            isFirst = true;
            backendWritedCount = 0;
            // 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
            // 向后端写入完成数据后，则从后端读取数据
        //            session.proxyBuffer.flip();
            // 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
//            session.change2ReadOpts();
        }
        return false;
    }

    @Override
    public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub
        super.clearFrontResouces(session, sessionCLosed);
    }

    @Override
    public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub
        super.clearBackendResouces(session, sessionCLosed);
    }

}
