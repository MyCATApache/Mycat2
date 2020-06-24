package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.sqlRecorder.SqlRecord;
import io.mycat.sqlRecorder.SqlRecorderRuntime;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShowStatCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@stat";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        List<SqlRecord> values = SqlRecorderRuntime.INSTANCE.getRecords().values().stream().sorted().collect(Collectors.toList());
        ResultSetBuilder builder = ResultSetBuilder.create();


        builder
                .addColumnInfo("STATEMENT", JDBCType.VARCHAR)
                .addColumnInfo("START_TIME", JDBCType.TIMESTAMP)
                .addColumnInfo("END_TIME", JDBCType.TIMESTAMP)
                .addColumnInfo("SQL_ROWS", JDBCType.BIGINT)
                .addColumnInfo("NET_IN_BYTES", JDBCType.BIGINT)
                .addColumnInfo("NET_OUT_BYTES", JDBCType.BIGINT)
                .addColumnInfo("PARSE_TIME", JDBCType.TIMESTAMP)
                .addColumnInfo("COMPILE_TIME", JDBCType.TIMESTAMP)
                .addColumnInfo("CBO_TIME", JDBCType.TIMESTAMP)
                .addColumnInfo("RBO_TIME", JDBCType.TIMESTAMP)
                .addColumnInfo("CONNECTION_POOL_TIME", JDBCType.TIMESTAMP)
                .addColumnInfo("CONNECTION_QUERY_TIME", JDBCType.TIMESTAMP);

        for (SqlRecord value : values) {
            String statement = value.getStatement();
            Timestamp startTime = new Timestamp(value.getStartTime());
            Timestamp endTime = new Timestamp(value.getEndTime());
            long sqlRows = value.getSqlRows();
            long netInBytes = value.getNetInBytes();
            long netOutBytes = value.getNetOutBytes();
            Timestamp parseTime = new Timestamp(value.getParseTime());
            Timestamp compileTime = new Timestamp(value.getCompileTime());
            Timestamp cboTime = new Timestamp(value.getCboTime());
            Timestamp rboTime = new Timestamp(value.getRboTime());
            Timestamp connectionPoolTime = new Timestamp(value.getConnectionPoolTime());
            Timestamp connectionQueryTIme = new Timestamp(value.getConnectionQueryTIme());

            builder.addObjectRowPayload(Arrays.asList(
                    statement,
                    startTime,
                    endTime,
                    sqlRows,
                    netInBytes,
                    netOutBytes,
                    parseTime,
                    compileTime,
                    cboTime,
                    rboTime,
                    connectionPoolTime,
                    connectionQueryTIme
            ));
        }
        response.sendResultSet(() -> builder.build());
    }
}