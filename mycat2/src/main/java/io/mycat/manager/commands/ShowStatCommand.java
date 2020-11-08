package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
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
    public void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception {
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
            double startTime = (value.getStartTime());
            double endTime =(value.getEndTime());
            double sqlRows = value.getSqlRows();
            double netInBytes = value.getNetInBytes();
            double netOutBytes = value.getNetOutBytes();
            double parseTime = (value.getParseTime());
            double compileTime =(value.getCompileTime());
            double cboTime =(value.getCboTime());
            double rboTime =(value.getRboTime());
            double connectionPoolTime = (value.getConnectionPoolTime());
            double connectionQueryTIme = (value.getConnectionQueryTime());
            double executionTime = value.getExecutionTime();
            double TOTAL_TIME = (value.getWholeTime());
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