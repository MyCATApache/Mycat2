//package io.mycat.mycat2.cmds.multinode;
//
//import io.mycat.mycat2.MySQLSession;
//import io.mycat.mycat2.MycatSession;
//import io.mycat.mycat2.cmds.AbstractMultiDNExeCmd;
//import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
//import io.mycat.mycat2.route.RouteResultsetNode;
//import io.mycat.mycat2.tasks.BackendIOTaskWithGenericResponse;
//import io.mycat.mycat2.tasks.DataNodeManager;
//import io.mycat.mycat2.tasks.HeapDataNodeMergeManager;
//import io.mycat.mycat2.tasks.multinode.PickOnlyOneInMultiNodeWithGenericResponse;
//import io.mycat.mysql.packet.ErrorPacket;
//import io.mycat.proxy.ProxyBuffer;
//import io.mycat.util.ErrorCode;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.nio.channels.SelectionKey;
//
///**
// * <b><code>DbInMultiServerCmd</code></b>
// * <p>
// * DbInMultiServer模式下的多节点执行Command类
// * </p>
// * <b>Creation Time:</b> 2018-01-20
// *
// * @author <a href="mailto:flysqrlboy@gmail.com">zhangsiwei</a>
// * @author <a href="mailto:karakapi@outlook.com">jamie</a>
// * @since 2.0
// */
//public class DbInMultiServerCmd extends AbstractMultiDNExeCmd {
//
//    public static final DbInMultiServerCmd INSTANCE = new DbInMultiServerCmd();
//
//    private static final DirectPassthrouhCmd inner = DirectPassthrouhCmd.INSTANCE;
//
//    private static final Logger logger = LoggerFactory.getLogger(DbInMultiServerCmd.class);
//
//    protected void broadcast(MycatSession mycatSession, RouteResultsetNode[] nodes)
//            throws IOException {
//        int size = nodes.length;
//        for (int i = 0; i < size; i++) {
//            RouteResultsetNode node = nodes[i];
//            DataNodeManager manager = mycatSession.merge = new HeapDataNodeMergeManager(mycatSession.getCurRouteResultset(), mycatSession);
//
//            mycatSession.getBackendByDataNodeName(node.getName(),
//                    (mysqlsession, sender, success, result) -> {
//                        try {
//                            if (success) {
//                                BackendIOTaskWithGenericResponse multiNodeBackendTask =
//                                        new PickOnlyOneInMultiNodeWithGenericResponse(mysqlsession,
//                                                mycatSession.getCurRouteResultset());
//                                multiNodeBackendTask.excecuteSQL(node.getStatement());
//                            } else {
//                                // 这个关闭方法会把所有backend都解除绑定并关闭
//                                manager.closeMutilBackendAndResponseError(success,
//                                        ((ErrorPacket) result));
//                            }
//                        } catch (Exception e) {
//                            String errmsg = "db in multi server cmd Error. " + e.getMessage();
//                            manager.closeMutilBackendAndResponseError(false,
//                                    ErrorCode.ERR_MULTI_NODE_FAILED, errmsg);
//                        }
//                    });
//        }
//    }
//
//    @Override
//    public boolean procssSQL(MycatSession mycatSession) throws IOException {
//        RouteResultsetNode[] nodes = mycatSession.getCurRouteResultset().getNodes();
//        if (nodes.length == 1) {
//            passthrough(mycatSession, nodes[0]);
//            return false;
//        } else if (nodes.length > 1) {
//            broadcast(mycatSession, nodes);
//            return false;
//        }
//        return true;
//    }
//
//    public boolean passthrough(MycatSession session, RouteResultsetNode node)
//            throws IOException {
//        /*
//         * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
//         */
//        session.clearReadWriteOpts();
//
//        session.getBackendByDataNodeName(node.getName(),
//                (mysqlsession, sender, success, result) -> {
//                    ProxyBuffer curBuffer = session.proxyBuffer;
//                    // 切换 buffer 读写状态
//                    curBuffer.flip();
//                    if (success) {
//                        // 没有读取,直接透传时,需要指定 透传的数据 截止位置
//                        curBuffer.readIndex = curBuffer.writeIndex;
//                        // 改变 owner，对端Session获取，并且感兴趣写事件
//                        session.giveupOwner(SelectionKey.OP_WRITE);
//                        try {
//                            mysqlsession.writeToChannel();
//                        } catch (IOException e) {
//                            session.closeBackendAndResponseError(mysqlsession, success,
//                                    ((ErrorPacket) result));
//                        }
//                    } else {
//                        session.closeBackendAndResponseError(mysqlsession, success,
//                                ((ErrorPacket) result));
//                    }
//                });
//        return false;
//    }
//
//
//    @Override
//    public boolean onFrontWriteFinished(MycatSession session) throws IOException {
//        session.setCurRouteResultset(null);
//        return inner.onFrontWriteFinished(session);
//    }
//
//    @Override
//    public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
//        session.setCurRouteResultset(null);
//        inner.clearFrontResouces(session, sessionCLosed);
//    }
//
//    /**
//     * @param session
//     * @return
//     * @throws IOException
//     */
//    @Override
//    public boolean onBackendResponse(MySQLSession session) throws IOException {
//        if (checkMutil(session)) {
//            throw new IOException("it is a bug , should use BackendIOTask mechanism");
//        }
//        return inner.onBackendResponse(session);
//    }
//
//
//    @Override
//    public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
//        if (checkMutil(session)) {
//            throw new IOException("it is a bug , should use BackendIOTask mechanism");
//        }
//        return inner.onBackendWriteFinished(session);
//    }
//
//
//    @Override
//    public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
//        if (checkMutil(session)) {
//            throw new IOException("it is a bug , should use BackendIOTask mechanism");
//        }
//        return inner.onBackendResponse(session);
//    }
//
//    @Override
//    public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
//        if (checkMutil(session)) {
//            logger.error("it is a bug , should use BackendIOTask mechanism");
//        }
//        inner.clearBackendResouces(session, sessionCLosed);
//    }
//
//    private boolean checkMutil(MySQLSession session) {
//        MycatSession mycatSession = session.getMycatSession();
//        return checkMutil(mycatSession);
//    }
//
//    private boolean checkMutil(MycatSession mycatSession) {
//        if (mycatSession != null) {
//            RouteResultsetNode[] nodes = mycatSession.getCurRouteResultset().getNodes();
//            return nodes != null && nodes.length > 1;
//        }
//        return false;
//    }
//}
