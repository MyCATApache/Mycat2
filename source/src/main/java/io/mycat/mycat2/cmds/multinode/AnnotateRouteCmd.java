//package io.mycat.mycat2.cmds.multinode;
//
//import io.mycat.mycat2.MycatSession;
//import io.mycat.mycat2.route.RouteResultsetNode;
//import io.mycat.mycat2.tasks.DataNodeManager;
//import io.mycat.mycat2.tasks.HeapDataNodeMergeManager;
//import io.mycat.mycat2.tasks.SQLQueryStream;
//import io.mycat.mysql.packet.ErrorPacket;
//import io.mycat.util.ErrorCode;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
///**
// * @todo 对于多节点处理, 重构, 例如抽取基类
// * cjw 2018.5.1
// */
//public class AnnotateRouteCmd extends DbInMultiServerCmd {
//
//    public static final AnnotateRouteCmd INSTANCE = new AnnotateRouteCmd();
//
//    private static final Logger logger = LoggerFactory.getLogger(AnnotateRouteCmd.class);
//
//    @Override
//    protected void broadcast(MycatSession mycatSession, RouteResultsetNode[] nodes)
//            throws IOException {
//        int size = nodes.length;
//        for (int i = 0; i < size; i++) {
//            RouteResultsetNode node = nodes[i];
//            //@todo refactor a class to handle the closeMutilBackendAndResponseError
//            DataNodeManager manager = new HeapDataNodeMergeManager(mycatSession.getCurRouteResultset(), mycatSession);
//            mycatSession.getBackendByDataNodeName(node.getName(),
//                    (mysqlsession, sender, success, result) -> {
//                        try {
//                            if (success) {
//                                SQLQueryStream sqlQueryStream = new SQLQueryStream(node.getName(), mysqlsession, manager);
//                                sqlQueryStream.fetchSQL(node.getStatement());
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
//}