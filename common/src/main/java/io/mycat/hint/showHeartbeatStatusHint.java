package io.mycat.hint;

public class showHeartbeatStatusHint extends HintBuilder {
    @Override
    public String getCmd() {
        return "showHeartbeatStatus";
    }
}