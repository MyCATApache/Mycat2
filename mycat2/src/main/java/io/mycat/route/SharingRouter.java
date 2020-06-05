package io.mycat.route;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLLimit;
import com.alibaba.fastsql.sql.ast.SQLOrderBy;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import io.mycat.DataNode;
import io.mycat.QueryBackendTask;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class SharingRouter implements SqlRouteChain {
    @Override
    public boolean handle(ParseContext parseContext) {
        List<SQLExprTableSource> leftTables = parseContext.startAndGetLeftTables();
        if (leftTables.size() == 1) {//means no join
            SQLExprTableSource singleDataSource = leftTables.get(0);
            SharingTableInfo sharingTableInfo = parseContext.getSharingTableInfo(singleDataSource);
            List<DataNode> dataNodes = sharingTableInfo.geDataNodes();
            if (dataNodes.size() == 1) {
                DataNode dataNode = dataNodes.get(0);
                parseContext.changeSchemaTable(singleDataSource, dataNode);
                parseContext.plan(HBTBuilder.create()
                        .from(dataNode.getTargetName(),
                        parseContext.getSqlStatement().toString()).build()
                );
                return true;
            } else {
                SQLStatement sqlStatement = parseContext.getSqlStatement();
                boolean noAgg = parseContext.isNoAgg();
                boolean noSubQuery = parseContext.isNoSubQuery();
                boolean noGroupBy = parseContext.isNoGroupBy();
                if (noAgg && noSubQuery && noGroupBy) {
                    MySqlSelectQueryBlock queryBlock = parseContext.unWapperToQueryBlock(sqlStatement);
                    if (queryBlock != null) {
                        SQLExpr where = queryBlock.getWhere();
                        HBTBuilder source = HBTBuilder.create();
                        if (where != null) {
                            dataNodes = parseContext.computeWhere(where);
                        }
                        boolean sharding = dataNodes.size() > 1;//是否分片
                        List<QueryBackendTask> queryBackendTasks = new ArrayList<>(dataNodes.size());
                        for (DataNode dataNode : dataNodes) {
                            parseContext.changeSchemaTable(singleDataSource, dataNode);
                            String targetName = dataNode.getTargetName();
                            String sql = parseContext.getSqlStatement().toString();
                            source.from(targetName, sql);
                        }

                        source = source.unionMore(!queryBlock.isDistinct());


                        SQLOrderBy orderBy = queryBlock.getOrderBy();

                        if (orderBy != null) {
                            source = source.order(parseContext.concertOrder(orderBy));
                        }

                        SQLLimit limitInfo = queryBlock.getLimit();
                        if (limitInfo != null) {
                            Long offset = parseContext.concertLimitGetOffset(limitInfo);
                            Long limit = parseContext.concertLimitGetLimit(limitInfo);

                            //offset limit不是数字
                            if (offset == null || limit == null) {
                                return false;
                            }
                            if (sharding){
                                limitInfo.setOffset(SQLExprUtils.fromJavaObject(0));
                                limitInfo.setRowCount(SQLExprUtils.fromJavaObject(BigInteger.valueOf(offset).add(BigInteger.valueOf(limit))));
                            }
                        }
                    }
                }
            }
        }
        return false;
    }
}