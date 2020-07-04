package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.sqlRecorder.SqlRecord;
import io.mycat.sqlRecorder.SqlRecorderRuntime;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        SqlRecorderRuntime.INSTANCE.reset();
        response.sendOk();
    }
}