package io.mycat.mycat2.route.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.beans.conf.TableDefBean;
import io.mycat.mycat2.beans.conf.TableDefBean.TypeEnum;
import io.mycat.mycat2.mpp.HavingCols;
import io.mycat.mycat2.mpp.MergeColumn;
import io.mycat.mycat2.mpp.OrderCol;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mycat2.route.RouteStrategy;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.proxy.ProxyRuntime;

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
    
    private String setMerge(MycatSession mycatSession, RouteResultset routeResultset) {
    	String sql = mycatSession.sqlContext.getRealSQL(0).toUpperCase();
		
		Map<String, Integer> aggrColumns = new HashMap<String, Integer>();
		
		StringBuffer sb = new StringBuffer(sql); 
		
		//将
		if (sql.indexOf("COUNT(") > 0) {
			if (sql.indexOf("COUNT0") == -1){
				sb.insert(sql.indexOf(")",sql.indexOf("COUNT(")) + 1, " AS COUNT0") ;
				sql = sb.toString();
			}
			int mergeType = MergeColumn.getMergeType("COUNT");
			aggrColumns.put("COUNT0", mergeType);
			
			routeResultset.setHasAggrColumn(true);
			routeResultset.setHavingColsName(null);
			routeResultset.setHavingCols(null);
			
		}
		if (sql.indexOf("SUM(") > 0) {
			if (sql.indexOf("SUM0") == -1){
				sb.insert(sql.indexOf(")",sql.indexOf("SUM(")) + 1, " AS SUM0") ;
				sql = sb.toString();
			}
			int mergeType = MergeColumn.getMergeType("SUM");
			aggrColumns.put("SUM0", mergeType);
			
			routeResultset.setHasAggrColumn(true);
			routeResultset.setHavingColsName(null);
			routeResultset.setHavingCols(null);
		}
		if (sql.indexOf("MAX(") > 0 ) {
			if (sql.indexOf("MAX0") == -1){
				sb.insert(sql.indexOf(")",sql.indexOf("MAX(")) + 1, " AS MAX0") ;
				sql = sb.toString();
			}
			int mergeType = MergeColumn.getMergeType("MAX");
			aggrColumns.put("MAX0", mergeType);
			
			routeResultset.setHasAggrColumn(true);
			routeResultset.setHavingColsName(null);
			routeResultset.setHavingCols(null);
		}
		if (sql.indexOf("MIN(") > 0) {
			if (sql.indexOf("MIN0") == -1){
				sb.insert(sql.indexOf(")",sql.indexOf("MIN(")) + 1, " AS MIN0") ;
				sql = sb.toString();
			}
			int mergeType = MergeColumn.getMergeType("MIN");
			aggrColumns.put("MIN0", mergeType);
			
			routeResultset.setHasAggrColumn(true);
			routeResultset.setHavingColsName(null);
			routeResultset.setHavingCols(null);
		}
		if (sql.indexOf("AVG(") > 0) {

			sb.insert(sql.indexOf(")",sql.indexOf("AVG(")) + 1, " AS MIN0") ;
			
			String col = (String) sb.subSequence(sql.indexOf("AVG(")+4, sql.indexOf(")",sql.indexOf("AVG(")));
			
			sql = sql.replace("AVG("+col+")", "SUM("+col+") AS AVG0SUM, COUNT("+col+") AS AVG0COUNT");
			
			int mergeType = MergeColumn.getMergeType("AVG");
			aggrColumns.put("AVG0", mergeType);
			
			aggrColumns.put("AVG0SUM", MergeColumn.MERGE_SUM);
			aggrColumns.put("AVG0COUNT", MergeColumn.MERGE_COUNT);
			
			routeResultset.setHasAggrColumn(true);
			routeResultset.setHavingColsName(null);
			routeResultset.setHavingCols(null);
		}
		
		//select count(*) as COUNT0,content from message group by content
		if(sql.indexOf("group by".toUpperCase()) > 0){
			String[] groupByCols = new String[]{"CONTENT"};
			routeResultset.setGroupByCols(groupByCols);
			
			routeResultset.setHavingColsName(null);
			routeResultset.setHavingCols(null);
			
			//select count(*) as COUNT0,content from message group by content having COUNT0 > 1;
			if(sql.indexOf("having".toUpperCase()) > 0){
				HavingCols having = new HavingCols("COUNT0", "0", ">");
				routeResultset.setHavingCols(having);
				routeResultset.setHasAggrColumn(true);
				routeResultset.setHavingColsName(new String[] { "count(*)", "COUNT0"});
			}
		}
		
		//order by
		//select * from message order by id;
		if(sql.indexOf("order by".toUpperCase()) > 0){
			LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
			for (int i = 0; i < 1; i ++) {
				//desc
				map.put("id", OrderCol.COL_ORDER_TYPE_DESC);
//				map.put(column.getName(), OrderCol.COL_ORDER_TYPE_ASC);
			}
			routeResultset.setOrderByCols(map);
		}
		
		if(routeResultset.getStatement().contains("limit")){
			routeResultset.setLimitStart(0);
			routeResultset.setLimitSize(2);
		}
		
		
		if (aggrColumns.size() > 0) {
			routeResultset.setMergeColumns(aggrColumns);
		}	
		return sql;
	}

}
