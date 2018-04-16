package io.mycat.mycat2.cmds.multinode;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.cmds.AbstractMultiDNExeCmd;
import io.mycat.mycat2.cmds.ComInitDB;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mycat2.tasks.HeapDataNodeMergeManager;
import io.mycat.mycat2.tasks.MyRowStream;
import io.mycat.mycat2.tasks.SQLQueryResultTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
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

    private static final Logger logger = LoggerFactory.getLogger(ComInitDB.class);
    private void broadcast(MycatSession mycatSession, RouteResultsetNode[] nodes) throws IOException {
        ProxyBuffer curBuffer = mycatSession.proxyBuffer;
        int readIndex = curBuffer.writeIndex;
        int readMark = curBuffer.readMark;
        int size = nodes.length;
        for (int i = 0; i < size; i++) {
            RouteResultsetNode node = nodes[i];
            /*
             * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
             */
//            curBuffer.readIndex = readIndex;
//            curBuffer.readMark = readMark;
            mycatSession.getBackendByDataNodeName(node.getName(), (mysqlsession, sender, success, result) -> {
                if (success) {
                    try {
                        MyRowStream stream = new MyRowStream(mysqlsession);
                        stream.setAbstractDataNodeMerge(mycatSession.merge);
                        stream.fetchStream(node.getStatement());
                    } catch (Exception e) {
                        mycatSession.closeBackendAndResponseError(mysqlsession, success,
                                ((ErrorPacket) result));
                    }
                } else {
                    mycatSession.closeBackendAndResponseError(mysqlsession, success,
                            ((ErrorPacket) result));
                }
            });
        }
    }

    @Override
    public boolean procssSQL(MycatSession mycatSession) throws IOException {
        DNBean dnBean = ProxyRuntime.INSTANCE.getConfig().getDNBean("dn1");
        logger.warn("dev 版本暂时还没有实现路由,默认路由到dn1,dn2");
        DNBean dnBean2 = ProxyRuntime.INSTANCE.getConfig().getDNBean("dn2");
        mycatSession.curRouteResultset = new RouteResultset("", (byte) 0);
        mycatSession.curRouteResultset.setNodes(new RouteResultsetNode[]{
                new RouteResultsetNode(dnBean.getName(), (byte) 1, mycatSession.sqlContext.getRealSQL(0)),
                new RouteResultsetNode(dnBean2.getName(), (byte) 1, mycatSession.sqlContext.getRealSQL(0))
        });
        RouteResultsetNode[] nodes = mycatSession.curRouteResultset.getNodes();
        if (true) {
            if (null != mycatSession.curRouteResultset) {
                mycatSession.merge = new HeapDataNodeMergeManager(mycatSession.curRouteResultset, mycatSession);
                if (nodes != null && nodes.length > 0) {
                    broadcast(mycatSession, nodes);
                    return false;
                }
            }
        } else {
            //lobal table optimization


        }
        return DirectPassthrouhCmd.INSTANCE.procssSQL(mycatSession);
    }

    @Override
    public boolean onBackendResponse(MySQLSession session) throws IOException {
//        session.proxyBuffer.
//

//        task.onSocketRead(session);
        // 首先进行一次报文的读取操作
//        if (!session.readFromChannel()) {
//            return false;
//        }
//        // 进行报文处理的流程化
//        boolean nextReadFlag = false;
//        do {
//            // 进行报文的处理流程
//            CommandHandler commandHandler = session.getMycatSession().commandHandler;
//            if (commandHandler ==  C)
//            nextReadFlag = commandHandler.procss(session);
//        } while (nextReadFlag);
//
//        // 获取当前是否结束标识
//        Boolean check = (Boolean) session.getSessionAttrMap()
//                .get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
//
//        MycatSession mycatSession = session.getMycatSession();
//        ProxyBuffer buffer = session.getProxyBuffer();
//
////        if (++executeCount < session.getMycatSession().curRouteResultset.getNodes().length) {
////            // DbInMultiServer模式下，不考虑show tables等DSL语句的话，只有对全局表的操作才会跨节点，也就是对全局表的DDL，DML语句，
////            // 而对每个节点的全局表操作完后返回的报文都是一样的，因此只需要拿最后一次的报文返回给客户端即可
////            if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
////                // 因为不是最后一个节点的返回报文，所以这里讲readmark设为readIndex，也就是丢弃掉这次报文（仅考虑全局表的DDL， DML返回报文）
////                // TODO show tables类的DSL语句就不适用，这个后续考虑时再优化
////                session.getProxyBuffer().readMark = session.getProxyBuffer().readIndex;
////            }
////            return false;
////        }
//
//        // 检查到当前已经完成,执行添加操作
//        if (null != check && check) {
//            // 当知道操作完成后，前段的注册感兴趣事件为读取
//            mycatSession.takeOwner(SelectionKey.OP_READ);
//        }
//        // 未完成执行继续读取操作
//        else {
//            // 直接透传报文
//            mycatSession.takeOwner(SelectionKey.OP_WRITE);
//        }
//        buffer.flip();
//       // executeCount = 0;
//        mycatSession.writeToChannel();
//        return true;


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
        SQLQueryResultTask task = new SQLQueryResultTask(session.getMycatSession().merge);
        session.setCurNIOHandler(task);
        session.proxyBuffer.flip();
        session.change2ReadOpts();
        return false;
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
