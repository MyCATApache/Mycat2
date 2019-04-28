package io.mycat.router;

import java.util.StringTokenizer;

public class RouteStrategyImpl implements RouteStrategy {
    final RouteResult routeResult = new RouteResult();

    @Override
    public RouteResult getRouteResult() {
        return routeResult;
    }

    @Override
    public boolean preprocessRoute(ByteArrayView view, String schema) {
        routeResult.setDataNodeName(null);
        StringBuilder sb = new StringBuilder();
        while (view.hasNext()) {
            sb.append(view.get());
        }
        StringTokenizer stringTokenizer = new StringTokenizer(sb.toString());
        while (stringTokenizer.hasMoreElements()) {
            if("form".equals(stringTokenizer.nextToken())){
                routeResult.setDataNodeName(stringTokenizer.nextToken());
                break;
            }
        }
        return true;
    }


    @Override
    public RouteResultFuture processRoute(RouteResult routeResult) {
        return null;
    }
}
