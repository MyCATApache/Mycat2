package io.mycat.commands;

import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;

import static io.mycat.commands.ExecuteCommand.getDetails;

public enum DistributedUpdateCommand implements MycatCommand{
    INSTANCE;
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        ExplainDetail details = getDetails(request,context, ExecuteType.UPDATE);
        response.execute(details);
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        ExplainDetail details = getDetails(request,context, ExecuteType.UPDATE);
        response.execute(details);
        return true;
    }

    @Override
    public String getName() {
        return "distributedUpdate";
    }
}