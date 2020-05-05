package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;
/**
 * @author Junwen Chen
 **/
public enum ExplainPlanCommand implements MycatCommand{
    INSTANCE;
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        String text = request.getText();
        MycatDBClientMediator client = MycatDBs.createClient(context);
        response.sendResultSet(client.executeRel(text), () -> client.explainRel(text));
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        String text = request.getText();
        MycatDBClientMediator client = MycatDBs.createClient(context);
        response.sendExplain(ExplainPlanCommand.class,client.explainRel(text));
        return true;
    }

    @Override
    public String getName() {
        return "explainPlan";
    }
}