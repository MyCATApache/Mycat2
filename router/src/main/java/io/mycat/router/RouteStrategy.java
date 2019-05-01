package io.mycat.router;

import io.mycat.sqlparser.util.ByteArrayView;

public interface RouteStrategy {
    RouteResult getRouteResult();
    RouteType preprocessRoute(ByteArrayView view,String schema);
    RouteResultFuture processRoute(RouteResult routeResult);
}
