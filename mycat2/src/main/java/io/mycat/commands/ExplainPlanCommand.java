package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.SQLRequest;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;

public class ExplainPlanCommand implements MycatCommand{
    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        String text = request.getText();
        MycatDBClientMediator client = MycatDBs.createClient(context);
        response.sendResultSet(client.executeRel(text));
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
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