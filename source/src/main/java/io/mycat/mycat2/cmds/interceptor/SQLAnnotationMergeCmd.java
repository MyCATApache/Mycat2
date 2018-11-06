package io.mycat.mycat2.cmds.interceptor;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.mpp.HavingCols;
import io.mycat.mycat2.mpp.MergeColumn;
import io.mycat.mycat2.mpp.OrderCol;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteResultsetNode;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.MergeAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/***
 * jamie 2018.5.1
 */
public class SQLAnnotationMergeCmd extends SQLAnnotationCmd {

    private static final Logger logger = LoggerFactory.getLogger(SQLAnnotationMergeCmd.class);

    @Override
    public boolean procssSQL(MycatSession session) throws IOException {

        logger.debug("=====>   SQLAnnotationMergeCmd   processSQL");

        BufferSQLContext context = session.sqlContext;
        if (BufferSQLContext.ANNOTATION_MERGE == context.getAnnotationType()) {
            MergeAnnotation mergeAnnotation = context.getMergeAnnotation();
            String[] dataNotes = mergeAnnotation.getDataNodes();
            RouteResultsetNode[] routeResultsetNodes = new RouteResultsetNode[dataNotes.length];
            String sql = context.getRealSQL(0);
            for (int i = 0; i < routeResultsetNodes.length; i++) {
                routeResultsetNodes[i] = new RouteResultsetNode(dataNotes[i], BufferSQLContext.SELECT_SQL, sql);
            }
            RouteResultset routeResultset = new RouteResultset(sql, BufferSQLContext.SELECT_SQL);
            setMerge(session, routeResultset);
            //todo 未完成
            if (mergeAnnotation.hasOrder()) {
                routeResultset.setLimitStart((int) mergeAnnotation.getLimitStart());
                routeResultset.setLimitSize((int) mergeAnnotation.getLimitSize());
            }
            routeResultset.setNodes(routeResultsetNodes);
            //session.setCurRouteResultset(routeResultset);
        }
        return super.procssSQL(session);
    }

    private String setMerge(MycatSession mycatSession, RouteResultset routeResultset) {
        String sql = mycatSession.sqlContext.getRealSQL(0).toUpperCase();

        Map<String, Integer> aggrColumns = new HashMap<String, Integer>();

        StringBuffer sb = new StringBuffer(sql);

        //将
        if (sql.indexOf("COUNT(") > 0) {
            if (sql.indexOf("COUNT0") == -1) {
                sb.insert(sql.indexOf(")", sql.indexOf("COUNT(")) + 1, " AS COUNT0");
                sql = sb.toString();
            }
            int mergeType = MergeColumn.getMergeType("COUNT");
            aggrColumns.put("COUNT0", mergeType);

            routeResultset.setHasAggrColumn(true);
            routeResultset.setHavingColsName(null);
            routeResultset.setHavingCols(null);

        }
        if (sql.indexOf("SUM(") > 0) {
            if (sql.indexOf("SUM0") == -1) {
                sb.insert(sql.indexOf(")", sql.indexOf("SUM(")) + 1, " AS SUM0");
                sql = sb.toString();
            }
            int mergeType = MergeColumn.getMergeType("SUM");
            aggrColumns.put("SUM0", mergeType);

            routeResultset.setHasAggrColumn(true);
            routeResultset.setHavingColsName(null);
            routeResultset.setHavingCols(null);
        }
        if (sql.indexOf("MAX(") > 0) {
            if (sql.indexOf("MAX0") == -1) {
                sb.insert(sql.indexOf(")", sql.indexOf("MAX(")) + 1, " AS MAX0");
                sql = sb.toString();
            }
            int mergeType = MergeColumn.getMergeType("MAX");
            aggrColumns.put("MAX0", mergeType);

            routeResultset.setHasAggrColumn(true);
            routeResultset.setHavingColsName(null);
            routeResultset.setHavingCols(null);
        }
        if (sql.indexOf("MIN(") > 0) {
            if (sql.indexOf("MIN0") == -1) {
                sb.insert(sql.indexOf(")", sql.indexOf("MIN(")) + 1, " AS MIN0");
                sql = sb.toString();
            }
            int mergeType = MergeColumn.getMergeType("MIN");
            aggrColumns.put("MIN0", mergeType);

            routeResultset.setHasAggrColumn(true);
            routeResultset.setHavingColsName(null);
            routeResultset.setHavingCols(null);
        }
        if (sql.indexOf("AVG(") > 0) {

            sb.insert(sql.indexOf(")", sql.indexOf("AVG(")) + 1, " AS MIN0");

            String col = (String) sb.subSequence(sql.indexOf("AVG(") + 4, sql.indexOf(")", sql.indexOf("AVG(")));

            sql = sql.replace("AVG(" + col + ")", "SUM(" + col + ") AS AVG0SUM, COUNT(" + col + ") AS AVG0COUNT");

            int mergeType = MergeColumn.getMergeType("AVG");
            aggrColumns.put("AVG0", mergeType);

            aggrColumns.put("AVG0SUM", MergeColumn.MERGE_SUM);
            aggrColumns.put("AVG0COUNT", MergeColumn.MERGE_COUNT);

            routeResultset.setHasAggrColumn(true);
            routeResultset.setHavingColsName(null);
            routeResultset.setHavingCols(null);
        }

        //select count(*) as COUNT0,content from message group by content
        if (sql.indexOf("group by".toUpperCase()) > 0) {
            String[] groupByCols = new String[]{"CONTENT"};
            routeResultset.setGroupByCols(groupByCols);

            routeResultset.setHavingColsName(null);
            routeResultset.setHavingCols(null);

            //select count(*) as COUNT0,content from message group by content having COUNT0 > 1;
            if (sql.indexOf("having".toUpperCase()) > 0) {
                HavingCols having = new HavingCols("COUNT0", "0", ">");
                routeResultset.setHavingCols(having);
                routeResultset.setHasAggrColumn(true);
                routeResultset.setHavingColsName(new String[]{"count(*)", "COUNT0"});
            }
        }

        //order by
        //select * from message order by id;
        if (sql.indexOf("order by".toUpperCase()) > 0) {
            LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
            for (int i = 0; i < 1; i++) {
                //desc
                map.put("id", OrderCol.COL_ORDER_TYPE_DESC);
//				map.put(column.getName(), OrderCol.COL_ORDER_TYPE_ASC);
            }
            routeResultset.setOrderByCols(map);
        }

        if (routeResultset.getStatement().contains("limit")) {
            routeResultset.setLimitStart(0);
            routeResultset.setLimitSize(2);
        }


        if (aggrColumns.size() > 0) {
            routeResultset.setMergeColumns(aggrColumns);
        }
        return sql;
    }

}
