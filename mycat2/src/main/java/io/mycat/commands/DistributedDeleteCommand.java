package io.mycat.commands;

import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.client.SQLRequest;
import io.mycat.util.Response;

public class DistributedDeleteCommand extends ExecuteCommand{
    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        ExplainDetail details = getDetails(request,context, ExecuteType.UPDATE);
        response.execute(details);
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
        ExplainDetail details = getDetails(request,context, ExecuteType.UPDATE);
        response.sendExplain(DistributedDeleteCommand.class,details);
        return true;
    }

    @Override
    public String getName() {
        return "distributedDelete";
    }
}