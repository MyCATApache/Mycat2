package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;

public class CommitCommand implements MycatCommand{
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        response.commit();
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        response.sendExplain(CommitCommand.class,"commit");
        return true;
    }

    @Override
    public String getName() {
        return "commit";
    }
}