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
//                .addColumnInfo("START_TIME", JDBCType.BIGINT)
//                .addColumnInfo("END_TIME", JDBCType.BIGINT)
//                .addColumnInfo("SQL_ROWS", JDBCType.BIGINT)
//                .addColumnInfo("NET_IN_BYTES", JDBCType.BIGINT)
//                .addColumnInfo("NET_OUT_BYTES", JDBCType.BIGINT)
//                .addColumnInfo("PARSE_TIME", JDBCType.BIGINT)
                .addColumnInfo("COMPILE_TIME", JDBCType.BIGINT)
                .addColumnInfo("RBO_TIME", JDBCType.BIGINT)
                .addColumnInfo("CBO_TIME", JDBCType.BIGINT)
                .addColumnInfo("CONNECTION_POOL_TIME", JDBCType.BIGINT)
                .addColumnInfo("CONNECTION_QUERY_TIME", JDBCType.BIGINT)
                .addColumnInfo("EXECUTION_TIME", JDBCType.BIGINT)
                .addColumnInfo("TOTAL_TIME", JDBCType.BIGINT)
        ;



        for (SqlRecord value : values) {
            String statement = value.getStatement();
            long startTime = (value.getStartTime());
            long endTime =(value.getEndTime());
            long sqlRows = value.getSqlRows();
            long netInBytes = value.getNetInBytes();
            long netOutBytes = value.getNetOutBytes();
            long parseTime = (value.getParseTime());
            long compileTime =(value.getCompileTime());
            long cboTime =(value.getCboTime());
            long rboTime =(value.getRboTime());
            long connectionPoolTime = (value.getConnectionPoolTime());
            long connectionQueryTIme = (value.getConnectionQueryTime());
            long executionTime = value.getExecutionTime();
            long TOTAL_TIME = (value.getWholeTime());
            builder.addObjectRowPayload(Arrays.asList(
                    statement,
//                    startTime,
//                    endTime,
//                    sqlRows,
//                    netInBytes,
//                    netOutBytes,
//                    parseTime,
                    compileTime,
                    rboTime,
                    cboTime,
                    connectionPoolTime,
                    connectionQueryTIme,
                    executionTime,
                    TOTAL_TIME
            ));
        }
        response.sendResultSet(() -> builder.build());
    }
}