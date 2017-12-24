package io.mycat.mycat2.route.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.beans.conf.TableDefBean;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mycat2.route.RouteStrategy;
import io.mycat.proxy.ProxyRuntime;

/**
 * <b><code>DBInMultiServerRouteStrategy</code></b>
 * <p>
 * DBInMultiServer模式下的路由策略，该模式下不允许跨库.
 * </p>
 * <b>Creation Time:</b> 2017-12-24
 * 
 * @author <a href="mailto:flysqrlboy@gmail.com">zhangsiwei</a>
 * @since 2.0
 */
public class DBInMultiServerRouteStrategy implements RouteStrategy {

    @Override
    public RouteResultset route(SchemaBean schema, int sqlType, String origSQL, String charset,
            MycatSession mycatSession) {

        Set<String> dataNodes = new HashSet<>();

        MycatConfig config = ProxyRuntime.INSTANCE.getConfig();

        for (int i = 0; i < mycatSession.sqlContext.getTableCount(); i++) {
            String tableName = mycatSession.sqlContext.getTableName(i);

            TableDefBean tableDefBean = config.getTableDefBean(tableName);
            if (tableDefBean != null) {
                dataNodes.add(tableDefBean.getDataNode());
            }
        }
        RouteResultset rrs = new RouteResultset(origSQL, sqlType);

        if (dataNodes.size() >= 1) {
            RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
            int i = 0;
            for (Iterator<String> it = dataNodes.iterator(); it.hasNext();) {
                nodes[i++] = new RouteResultsetNode(it.next(), sqlType, origSQL);
            }
            rrs.setNodes(nodes);
            return rrs;
        }

        return rrs;
    }

}
