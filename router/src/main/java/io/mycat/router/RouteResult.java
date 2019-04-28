package io.mycat.router;

public class RouteResult {
    String dataNodeName;
    boolean changed;

    public boolean isSQLChanged() {
        return changed;
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
