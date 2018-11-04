package io.mycat.mycat2.cmds.interceptor;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.proxy.ProxyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/***
 * jamie 2018.4.17
 */
public class SQLAnnotationDatanodeCmd extends SQLAnnotationCmd {

    private static final Logger logger = LoggerFactory.getLogger(SQLAnnotationDatanodeCmd.class);

    @Override
    public boolean procssSQL(MycatSession session) throws IOException {

        logger.debug("=====>   SQLAnnotationDatanodeCmd   processSQL");

        BufferSQLContext context = session.sqlContext;
        if (BufferSQLContext.ANNOTATION_DATANODE == context.getAnnotationType()) {
            long dn = context.getAnnotationValue(BufferSQLContext.ANNOTATION_DATANODE);
            DNBean dnBean = ProxyRuntime.INSTANCE.getConfig().getDNBean(dn);
            String sql = context.getRealSQL(0);
            RouteResultset routeResultset = new RouteResultset(sql, BufferSQLContext.SELECT_SQL);
            routeResultset.setNodes(new RouteResultsetNode[]{new RouteResultsetNode(dnBean.getName(), BufferSQLContext.SELECT_SQL, sql)});
            // session.setCurRouteResultset(routeResultset);
        }

        return super.procssSQL(session);
    }

}
