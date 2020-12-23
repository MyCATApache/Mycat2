package io.mycat.hint;

public class ShowConnectionsHint extends HintBuilder {
    @Override
    public String getCmd() {
        return "showConnections";
    }
}