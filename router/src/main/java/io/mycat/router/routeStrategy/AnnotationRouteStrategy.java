/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router.routeStrategy;

import io.mycat.MycatExpection;
import io.mycat.beans.mycat.GlobalTable;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.beans.mycat.MycatTableRule;
import io.mycat.beans.mycat.ShardingDbTable;
import io.mycat.beans.mycat.ShardingTableTable;
import io.mycat.router.DynamicAnnotationResult;
import io.mycat.router.ResultRoute;
import io.mycat.router.RouteContext;
import io.mycat.router.RouteStrategy;
import io.mycat.router.routeResult.GlobalTableWriteResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.router.routeResult.SubTableResultRoute;
import io.mycat.sqlparser.util.BufferSQLContext;
import io.mycat.sqlparser.util.SQLUtil;
import java.util.List;

/**
 * @author jamie12221
 * @date 2019-05-05 16:54
 **/
public class AnnotationRouteStrategy implements RouteStrategy<RouteContext> {

  @Override
  public ResultRoute route(MycatSchema schema, String sql, RouteContext context) {
    BufferSQLContext sqlContext = context.getSqlContext();
    if (sqlContext.getTableCount() == 1) {
      String tableName = sqlContext.getTableName(0);
      MycatTable o = schema.getTableByTableName(tableName);
      switch (o.getType()) {
        case GLOBAL: {
          GlobalTable table = (GlobalTable) o;
          if (sqlContext.isSimpleSelect()) {
            OneServerResultRoute result = new OneServerResultRoute();
            String dataNode = table.getDataNodes().get(0);//@todo 负载均衡
            result.setDataNode(dataNode);
            return result;
          } else {
            GlobalTableWriteResultRoute result = new GlobalTableWriteResultRoute();
            result.setSql(sql);
            List<String> dataNodes = table.getDataNodes();
            String[] strings = dataNodes.toArray(new String[dataNodes.size()]);
            result.setDataNodes(strings);
            return result;
          }
        }
        case SHARING_DATABASE: {
          ShardingDbTable table = (ShardingDbTable) o;
          MycatTableRule rule = table.getRule();
          DynamicAnnotationResult matchResult = rule.getMatcher().match(sql);
          context.clearDataNodeIndexes();
          rule.getRoute().route(matchResult, 0, rule.getRuleAlgorithm(), context);
          int index = context.getIndex();
          int[] indexes = context.getIndexes();
          if (index == -1) {
            return context.getSqlParseRouteRouteStrategy().route(schema, sql, context);
          } else if (index > -1) {
            String dataNode = table.getDataNodes().get(index);
            OneServerResultRoute result = new OneServerResultRoute();
            result.setDataNode(dataNode);
            result.setSql(sql);
            return result;
          } else {
            throw new MycatExpection("unknown state!");
          }
        }
        case SHARING_TABLE: {
          ShardingTableTable table = (ShardingTableTable) o;
          MycatTableRule rule = table.getRule();
          DynamicAnnotationResult matchResult = rule.getMatcher().match(sql);
          context.clearDataNodeIndexes();
          rule.getRoute().route(matchResult, 0, rule.getRuleAlgorithm(), context);
          int index = context.getIndex();
          int[] indexes = context.getIndexes();
          if (index == -1) {
            return context.getSqlParseRouteRouteStrategy().route(schema, sql, context);
          } else if (index > -1) {
            String actTableName = table.getSubTable(index);
            CharSequence actSQL = SQLUtil.adjustmentSQL(sqlContext, true, tableName,
                actTableName);
            SubTableResultRoute result = new SubTableResultRoute();
            result.setSql(actSQL);
            result.setDataNode(table.getDataNodes().get(0));
            return result;
          } else {
            throw new MycatExpection("unknown state!");
          }
        }
        case SHARING_DATABASE_TABLE:
        default:
      }
    }
    return context.getSqlParseRouteRouteStrategy()
               .route(schema, sql, context);

  }

}
