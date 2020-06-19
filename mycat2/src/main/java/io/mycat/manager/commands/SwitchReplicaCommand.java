package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.JsonUtil;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class SwitchReplicaCommand  implements ManageCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchInstanceCommand.class);
    @Override
    public String statement() {
        return "switch @@backend.replica = {name:xxx} ";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if (request.getText().toLowerCase().startsWith("switch @@backend.replica")){
            handle(request, context, response);
            return true;
        }
        return false;
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        String json = request.getText().split("=")[1];
        Map from = JsonUtil.from(json, Map.class);
        String name = Objects.requireNonNull((String) from.get("name"),"name required");
        ReplicaSelectorRuntime.INSTANCE.notifySwitchReplicaDataSource(name);
        response.sendOk();
    }
}