package io.mycat.commands;

import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;

import static io.mycat.commands.ExecuteCommand.getDetails;
/**
 * @author Junwen Chen
 **/
public enum DistributedInsertCommand  implements MycatCommand{
    INSTANCE;
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        ExplainDetail details = getDetails(request,context, ExecuteType.INSERT);
        response.execute(details);
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        ExplainDetail details = getDetails(request,context, ExecuteType.INSERT);
        response.sendExplain(DistributedInsertCommand.class,details);
        return true;
    }

    @Override
    public String getName() {
        return "distributedInsert";
    }
}