package io.mycat.router;

public class RouteResult {

  String dataNodeName;
  boolean changed;
  RouteType routeType = RouteType.PURE_QUERY_SINGLE_NODE;

  public boolean isSQLChanged() {
    return changed;
  }

  public RouteType isPureQueryReadNode() {
    return routeType;
  }

  public void reset() {
    dataNodeName = null;
    changed = false;
    routeType = RouteType.PURE_QUERY_SINGLE_NODE;

  }

  public RouteType getRouteType() {
    return routeType;
  }

  public void setRouteType(RouteType routeType) {
    this.routeType = routeType;
  }

  public void setChanged(boolean changed) {
    this.changed = changed;
  }

  public String getDataNodeName() {
    return dataNodeName;
  }

  public void setDataNodeName(String dataNodeName) {
    this.dataNodeName = dataNodeName;
  }
}
