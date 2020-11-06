package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import io.mycat.util.Response;

public class ResetStatCommand implements ManageCommand {
    @Override
    public String statement() {
        return "reset @@stat";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception {
        SqlRecorderRuntime.INSTANCE.reset();
        response.sendOk();
    }
}