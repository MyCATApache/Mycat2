package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.SQLRequest;
import io.mycat.util.Response;

import java.nio.CharBuffer;
import java.util.Map;

public class ExplainSqlCommand implements MycatCommand{
    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        String statement = (String) request.getContext().get("statement");
        Map<String, Object> context1 = request.getContext();
        context1.put("doExplain","true");
        request.getUserSpace().execute(request.getSessionId(),
                context,CharBuffer.wrap(statement),context1,response);
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
        String statement = (String) request.getContext().get("statement");
        response.sendExplain(ExplainSqlCommand.class, getName()+":"+statement);
        return true;
    }

    @Override
    public String getName() {
        return "explainSql";
    }
}