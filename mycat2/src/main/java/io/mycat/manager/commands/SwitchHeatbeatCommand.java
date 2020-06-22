package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.Response;

public class SwitchHeatbeatCommand implements ManageCommand {
    @Override
    public String statement() {
        return "switch @@backend.heartbeat = {true|false}";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        try {
            String value = request.getText().split("=")[0];
            if (Boolean.parseBoolean(value)) {
                ReplicaSelectorRuntime.INSTANCE.restartHeatbeat();
            }else {
                ReplicaSelectorRuntime.INSTANCE.stopHeartBeat();
            }
        } catch (Throwable e) {
            response.sendError(e);
        }
    }
}