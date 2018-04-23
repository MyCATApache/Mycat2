package io.mycat.mycat2.cmds.multinode;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.cmds.AbstractMultiDNExeCmd;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.hbt.TableMeta;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mycat2.tasks.DataNodeManager;
import io.mycat.mycat2.tasks.HeapDataNodeMergeManager;
import io.mycat.mycat2.tasks.SQLQueryStream;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ErrorCode;
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
 * @author <a href="mailto:karakapi@outlook.com">jamie</a>
 * @since 2.0
 */
public class DbInMultiServerCmd extends AbstractMultiDNExeCmd {

    public static final DbInMultiServerCmd INSTANCE = new DbInMultiServerCmd();

    private static final DirectPassthrouhCmd inner = DirectPassthrouhCmd.INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(DbInMultiServerCmd.class);

    private static void broadcast(MycatSession mycatSession, RouteResultsetNode[] nodes) throws IOException {
        int size = nodes.length;
        for (int i = 0; i < size; i++) {
            RouteResultsetNode node = nodes[i];
            DataNodeManager manager = mycatSession.merge;
            mycatSession.getBackendByDataNodeName(node.getName(), (mysqlsession, sender, success, result) -> {
                try {
                    if (success) {
                        SQLQueryStream stream = new SQLQueryStream(node.getName(), mysqlsession, manager);
                        manager.addSQLQueryStream(stream);
                        stream.fetchSQL(node.getStatement());
                    } else {
                        //这个关闭方法会把所有backend都解除绑定并关闭
                        manager.closeMutilBackendAndResponseError(success,
                                ((ErrorPacket) result));
                    }
                } catch (Exception e) {
                    String errmsg = "db in multi server cmd Error. " + e.getMessage();
                    manager.closeMutilBackendAndResponseError(false, ErrorCode.ERR_MULTI_NODE_FAILED, errmsg);
                }
            });
        }

    }

    @Override
    public boolean procssSQL(MycatSession mycatSession) throws IOException {
        if (mycatSession.getCurRouteResultset() != null) {
            RouteResultsetNode[] nodes = mycatSession.getCurRouteResultset().getNodes();
            if (nodes.length == 1) {
                transparent(mycatSession, nodes[0].getName());
                return false;
            }
        } else {
            DNBean dnBean = ProxyRuntime.INSTANCE.getConfig().getDNBean("dn1");
            DNBean dnBean2 = ProxyRuntime.INSTANCE.getConfig().getDNBean("dn2");
            String sql = mycatSession.sqlContext.getRealSQL(0);
            RouteResultset curRouteResultset = new RouteResultset(sql, (byte) 0);
            curRouteResultset.setNodes(
                    new RouteResultsetNode[]{
                            new RouteResultsetNode(dnBean.getName(), (byte) 1, sql),
                            new RouteResultsetNode(dnBean2.getName(), (byte) 1, sql)});
            mycatSession.setCurRouteResultset(curRouteResultset);
            mycatSession.merge = new HeapDataNodeMergeManager(mycatSession.getCurRouteResultset(), mycatSession);
            RouteResultsetNode[] nodes = mycatSession.merge.getRouteResultset().getNodes();
            if (nodes != null && nodes.length > 0) {
                broadcast(mycatSession, nodes);
                return false;
            }
        }
        return inner.procssSQL(mycatSession);
    }

    public boolean transparent(MycatSession session, String dataNodeName) throws IOException {
        /*
         * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
         */
        session.clearReadWriteOpts();

        session.getBackendByDataNodeName(dataNodeName, (mysqlsession, sender, success, result) -> {

            ProxyBuffer curBuffer = session.proxyBuffer;
            // 切换 buffer 读写状态
            curBuffer.flip();
            if (success) {
                // 没有读取,直接透传时,需要指定 透传的数据 截止位置
                curBuffer.readIndex = curBuffer.writeIndex;
                // 改变 owner，对端Session获取，并且感兴趣写事件
                session.giveupOwner(SelectionKey.OP_WRITE);
                try {
                    mysqlsession.writeToChannel();
                } catch (IOException e) {
                    session.closeBackendAndResponseError(mysqlsession, success, ((ErrorPacket) result));
                }
            } else {
                session.closeBackendAndResponseError(mysqlsession, success, ((ErrorPacket) result));
            }
        });
        return false;
    }


    @Override
    public boolean onFrontWriteFinished(MycatSession session) throws IOException {
        if (checkMutil(session)) {
            //@todo 改为迭代器实现
            TableMeta tableMeta = ((HeapDataNodeMergeManager) session.merge).getTableMeta();
            if (session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_KEY_MERGE_OVER_FLAG.getKey()) && !tableMeta.isWriteFinish()) {
                ProxyBuffer buffer = session.proxyBuffer;
                buffer.reset();
                tableMeta.writeRowData(buffer);
                buffer.flip();
                buffer.readIndex = buffer.writeIndex;
                session.takeOwner(SelectionKey.OP_WRITE);
                session.writeToChannel();
                return false;
            } else {
                session.merge.onfinished();
                session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_MERGE_OVER_FLAG.getKey());
                session.proxyBuffer.flip();
                session.takeOwner(SelectionKey.OP_READ);
                return true;
            }
        } else {
            session.setCurRouteResultset(null);
            return inner.onFrontWriteFinished(session);
        }
    }

    @Override
    public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
        if (checkMutil(session)) {
            session.merge.clearResouces();
        }
        session.setCurRouteResultset(null);
        inner.clearFrontResouces(session, sessionCLosed);
    }

    /**
     * @param session
     * @return
     * @throws IOException
     */
    @Override
    public boolean onBackendResponse(MySQLSession session) throws IOException {
        if (checkMutil(session)) {
            throw new IOException("it is a bug , should call Stream");
        }
        return inner.onBackendResponse(session);
    }


    @Override
    public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
        if (checkMutil(session)) {
            throw new IOException("it is a bug , should call Stream");
        }
        return inner.onBackendWriteFinished(session);
    }


    @Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
        if (checkMutil(session)) {
            throw new IOException("it is a bug , should call Stream");
        }
        return inner.onBackendResponse(session);
    }

    @Override
    public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
        if (checkMutil(session)) {
            logger.error("it is a bug , should call Stream");
        }
        inner.clearBackendResouces(session, sessionCLosed);
    }

    private boolean checkMutil(MySQLSession session) {
        MycatSession mycatSession = session.getMycatSession();
        return checkMutil(mycatSession);
    }

    private boolean checkMutil(MycatSession mycatSession) {
        if (mycatSession != null) {
            DataNodeManager manager = mycatSession.merge;
            if (manager != null) {
                return manager.isMultiBackendMoreOne();
            }
        }
        return false;
    }
}
