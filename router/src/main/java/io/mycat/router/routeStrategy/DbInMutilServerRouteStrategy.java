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

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.router.ResultRoute;
import io.mycat.router.RouteContext;
import io.mycat.router.RouteStrategy;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.sqlparser.util.BufferSQLContext;

/**
 * @author jamie12221
 * @date 2019-05-05 16:54
 **/
public class DbInMutilServerRouteStrategy implements RouteStrategy<RouteContext> {

  @Override
  public ResultRoute route(MycatSchema schema, CharSequence sql, RouteContext context) {
    BufferSQLContext sqlContext = context.getSqlContext();
    int sqlCount = sqlContext.getSQLCount();
    if (sqlCount == 1 && sqlContext.getTableCount() == 1) {
      OneServerResultRoute result = new OneServerResultRoute();
      String tableName = sqlContext.getTableName(0);
      MycatTable tableByTable = schema.getTableByTableName(tableName);
      String dataNode = tableByTable.getDataNodes().get(0);
      result.setDataNode(dataNode);
      result.setSql(sql);
      return result;
    }else {
     return context.getSqlParseRouteRouteStrategy()
          .route(schema,sql,context);
    }
  }
}
