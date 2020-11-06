package io.mycat.manager.commands;

import io.mycat.MetaClusterCurrent;
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

public class SwitchInstanceCommand implements ManageCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchInstanceCommand.class);

    @Override
    public String statement() {
        return "switch @@backend.instance = {name:'xxx' ,alive:'true' ,readable:'true'}";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) throws Exception {
        if (request.getText().toLowerCase().startsWith("switch @@backend.instance")) {
            handle(request, context, response);
            return true;
        }
        return false;
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception {
        String json = request.getText().split("=")[1];
        Map from = JsonUtil.from(json, Map.class);

        String name = Objects.requireNonNull((String) from.get("name"), "name required");
        String readable = (String) from.get("readable");
        String alive = (String) from.get("alive");

        ReplicaSelectorRuntime replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);

        Map<String, PhysicsInstance> physicsInstanceMap = replicaSelectorRuntime.getPhysicsInstanceMap();
        PhysicsInstance physicsInstance = Objects.requireNonNull(physicsInstanceMap.get(name), "name is not existed");

        if (readable != null) {
            try {
                physicsInstance.notifyChangeSelectRead(Boolean.parseBoolean(readable));
            } catch (Throwable e) {
                LOGGER.error("update notifyChangeSelectRead name:{} readable:{} fail:{}", name, readable, e);
                response.sendError(e);
                return;
            }
        }
        if (readable != null) {
            try {
                physicsInstance.notifyChangeAlive(Boolean.parseBoolean(alive));
            } catch (Throwable e) {
                LOGGER.error("update notifyChangeAlive name:{} alive:{} fail:{}", name, alive, e);
                response.sendError(e);
                return;
            }
        }
        response.sendOk();
    }
}