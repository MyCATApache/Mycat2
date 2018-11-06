package io.mycat.mycat2.route.impl;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.beans.conf.TableDefBean;
import io.mycat.mycat2.beans.conf.TableDefBean.TypeEnum;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mycat2.route.RouteStrategy;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.proxy.ProxyRuntime;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * <b><code>DBInMultiServerRouteStrategy</code></b>
 * <p>
 * DBInMultiServer模式下的路由策略，该模式下除全局表外不允许跨库.
 * </p>
 * <b>Creation Time:</b> 2017-12-24
 * 
 * @author <a href="mailto:flysqrlboy@gmail.com">zhangsiwei</a>
 * @since 2.0
 */
public class DBInMultiServerRouteStrategy implements RouteStrategy {

    @Override
    public RouteResultset route(SchemaBean schema, byte sqlType, String origSQL, String charset,
            MycatSession mycatSession) {

        Set<String> dataNodes = new HashSet<>();
        Set<String> globalDataNodes = new HashSet<>(); // 全局表的datanode
        MycatConfig config = ProxyRuntime.INSTANCE.getConfig();
        boolean existGlobalTable = false;
        for (int i = 0; i < mycatSession.sqlContext.getTableCount(); i++) {
            String tableName = mycatSession.sqlContext.getTableName(i);

            TableDefBean tableDefBean = config.getTableDefBean(tableName);
            if (tableDefBean != null) {
                if (tableDefBean.getType() == TypeEnum.global) {
                    if (!existGlobalTable) {
                        existGlobalTable = true;
                    }
                    globalDataNodes.addAll(tableDefBean.getDataNodes());
                } else {
                    dataNodes.addAll(tableDefBean.getDataNodes());
                }
            } else {
                dataNodes.add(schema.getDefaultDataNode());
            }
        }
        // 就全局表而言，只有查询操作不需要跨节点，其他操作都要
        if (sqlType != BufferSQLContext.SELECT_SQL
                && sqlType != BufferSQLContext.SELECT_FOR_UPDATE_SQL
                && sqlType != BufferSQLContext.SELECT_INTO_SQL) {
            dataNodes.addAll(globalDataNodes);
        } else {
            if (!globalDataNodes.isEmpty()) {
                dataNodes.retainAll(globalDataNodes);
            }
        }
        RouteResultset rrs = new RouteResultset(origSQL, sqlType);
//        String changSql = setMerge(mycatSession, rrs);
        if (existGlobalTable) {
            rrs.setGlobalTable(true);
        }
        if (dataNodes.size() >= 1) {
            RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
            int i = 0;
            for (Iterator<String> it = dataNodes.iterator(); it.hasNext();) {
                nodes[i++] = new RouteResultsetNode(it.next(), sqlType, origSQL);
            }
            rrs.setNodes(nodes);
            return rrs;
        } else {
            // 使用默认datanode
            RouteResultsetNode[] nodes = new RouteResultsetNode[1];
            nodes[0] = new RouteResultsetNode(schema.getDefaultDataNode(), sqlType, origSQL);
            rrs.setNodes(nodes);
            return rrs;
        }
    }
}
