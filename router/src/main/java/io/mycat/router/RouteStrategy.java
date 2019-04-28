package io.mycat.router;

public interface RouteStrategy {
    RouteResult getRouteResult();
    boolean preprocessRoute(ByteArrayView view,String schema);
    RouteResultFuture processRoute(RouteResult routeResult);
}
