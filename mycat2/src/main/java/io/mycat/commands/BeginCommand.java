package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;

public class BeginCommand implements MycatCommand{
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        response.begin();
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        response.sendExplain(BeginCommand.class,"begin");
        return true;
    }

    @Override
    public String getName() {
        return "begin";
    }
}