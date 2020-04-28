package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.SQLRequest;
import io.mycat.util.Response;

public class CommitCommand implements MycatCommand{
    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        response.commit();
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
        response.sendExplain(CommitCommand.class,"commit");
        return true;
    }

    @Override
    public String getName() {
        return "commit";
    }
}