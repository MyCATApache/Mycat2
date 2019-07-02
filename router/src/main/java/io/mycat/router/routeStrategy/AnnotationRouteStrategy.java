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

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.beans.mycat.MycatTableRule;
import io.mycat.beans.mycat.ShardingDbTable;
import io.mycat.router.DynamicAnnotationResult;
import io.mycat.router.MycatProxyStaticAnnotation;
import io.mycat.router.ResultRoute;
import io.mycat.router.RouteContext;
import io.mycat.router.RouteStrategy;
import io.mycat.router.RuleAlgorithm;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.sqlparser.util.BufferSQLContext;
import java.util.Objects;

/**
 * @author jamie12221 date 2019-05-05 16:54
 **/
public class AnnotationRouteStrategy implements RouteStrategy<RouteContext> {

  @Override
  public ResultRoute route(MycatSchema schema, String sql, RouteContext context) {
    BufferSQLContext sqlContext = context.getSqlContext();
    if (sqlContext.getTableCount() == 1) {
      String tableName = sqlContext.getTableName(0);
      MycatTable o = schema.getTableByTableName(tableName);
      if (o == null) {
        throw new MycatException(tableName + " table is nor existed!");
      }
      Objects.requireNonNull(o.getType());
      switch (o.getType()) {
        case GLOBAL: {
          throw new MycatException("unsupport global table");
//          GlobalTable table = (GlobalTable) o;
//          if (sqlContext.isSimpleSelect()) {
//            OneServerResultRoute result = new OneServerResultRoute();
//            int index = ThreadLocalRandom.current().nextInt(0, table.getDataNodes().size());
//            String dataNode = table.getDataNodes().get(index);//@todo 负载均衡
//            result.setSql(sql);
//            result.setDataNode(dataNode);
//            return result;
//          } else {
//            GlobalTableWriteResultRoute result = new GlobalTableWriteResultRoute();
//            result.setSql(sql);
//            List<String> dataNodes = table.getDataNodes();
//            result.setMaster(dataNodes.get(0));
//            result.setDataNodes(dataNodes.subList(1, dataNodes.size()));
//            return result;
//          }
        }
        case SHARING_DATABASE: {
          ShardingDbTable table = (ShardingDbTable) o;
          MycatTableRule rule = table.getRule();

          RuleAlgorithm ruleAlgorithm = rule.getRuleAlgorithm();
          MycatProxyStaticAnnotation sa = context.getStaticAnnotation();
          if (sa != null) {
            if (sa.getShardingKey() != null && sa.getShardingRangeKeyStart() == null
                && sa.getShardingRangeKeyEnd() == null) {
              int calculate = ruleAlgorithm.calculate(sa.getShardingKey());
              OneServerResultRoute result = new OneServerResultRoute();
              return result.setSql(sql).setDataNode(table.getDataNodes().get(calculate));
            } else if (sa.getShardingKey() == null && sa.getShardingRangeKeyStart() != null
                && sa.getShardingRangeKeyEnd() != null) {
              int[] keys = ruleAlgorithm
                  .calculateRange(sa.getShardingRangeKeyStart(), sa.getShardingRangeKeyEnd());
              if (keys.length == 1) {
                OneServerResultRoute result = new OneServerResultRoute();
                return result.setSql(sql).setDataNode(table.getDataNodes().get(keys[0]));
              }
            } else if (sa.getShardingKey() != null && sa.getShardingRangeKeyStart() != null
                && sa.getShardingRangeKeyEnd() != null) {
              int calculate = ruleAlgorithm.calculate(sa.getShardingKey());
              int[] keys = ruleAlgorithm
                  .calculateRange(sa.getShardingRangeKeyStart(), sa.getShardingRangeKeyEnd());
              if (keys.length == 1 && calculate == keys[0]) {
                OneServerResultRoute result = new OneServerResultRoute();
                return result.setSql(sql).setDataNode(table.getDataNodes().get(calculate));
              }
            }
          }
          DynamicAnnotationResult matchResult = rule.getMatcher().match(sql);
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
            throw new MycatException("unknown state!");
          }
        }
        case SHARING_TABLE: {
          throw new MycatException("unsupport subtable table");
//          ShardingTableTable table = (ShardingTableTable) o;
//          MycatTableRule rule = table.getRule();
//          DynamicAnnotationResult matchResult = rule.getMatcher().match(sql);
//          context.clear();
//          rule.getRoute().route(matchResult, 0, rule.getRuleAlgorithm(), context);
//          int index = context.getIndex();
//          int[] indexes = context.getIndexes();
//          if (index == -1) {
//            return context.getSqlParseRouteRouteStrategy().route(schema, sql, context);
//          } else if (index > -1) {
//            String actTableName = table.getSubTable(index);
//            CharSequence actSQL = SQLUtil.adjustmentSQL(sqlContext, true, tableName,
//                actTableName);
//            SubTableResultRoute result = new SubTableResultRoute();
//            result.setSql(actSQL);
//            result.setDataNode(table.getDataNodes().get(0));
//            return result;
//          } else {
//            throw new MycatException("unknown state!");
//          }
        }
        case SHARING_DATABASE_TABLE:
        default:
      }
    }
    throw new MycatException("unsupport sql in annotation route");

  }

}
