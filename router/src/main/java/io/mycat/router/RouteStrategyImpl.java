package io.mycat.router;

import io.mycat.sqlparser.util.BufferSQLContext;
import io.mycat.sqlparser.util.BufferSQLParser;
import io.mycat.sqlparser.util.ByteArrayView;

public class RouteStrategyImpl implements RouteStrategy {

  final RouteResult routeResult = new RouteResult();
  final BufferSQLParser sqlParser = new BufferSQLParser();
  final BufferSQLContext context = new BufferSQLContext();

  @Override
  public RouteResult getRouteResult() {
    return routeResult;
  }

  @Override
  public RouteType preprocessRoute(ByteArrayView view, String schema) {
    routeResult.reset();
    sqlParser.parse(view, context);
    RouteType routeType = RouteType.OTHER;
    byte sqlType = context.getSQLType();
    if (sqlType == BufferSQLContext.SELECT_SQL) {
      if (context.isHasWhere() & !context.isHasBetween() && !context.isHasCompare()
              && !context.isHasJoin() && !context.isHasSubQuery() && !context.isHasUnion()
              && context.getTableCount() == 1) {
        routeType = RouteType.PURE_QUERY_SINGLE_NODE;
      } else {
        routeType = RouteType.COMPLEX_QUERY;
      }
    }
    routeResult.setRouteType(routeType);
    return routeType;
  }

  @Override
  public RouteResultFuture processRoute(RouteResult routeResult) {
    return null;
  }
}
