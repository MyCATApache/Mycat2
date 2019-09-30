package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.lib.impl.JdbcLib;
import io.mycat.lib.impl.Response;

import java.util.function.Supplier;

public class JdbcExport implements InstructionSet {


    public static Response beginOnJdbc() {
        return JdbcLib.begin();
    }

    public static Response commitOnJdbc() {
        return JdbcLib.commit();
    }

    public static Response rollbackOnJdbc() {
        return JdbcLib.rollback();
    }

    public static Response responseQueryOnJdbcByDataSource(String dataSource, String sql) {
        return JdbcLib.responseQueryOnJdbcByDataSource(dataSource, sql);
    }

    public static Response responseQueryOnJdbcByDataSource(String dataSource, String... sql) {
        return JdbcLib.responseQueryOnJdbcByDataSource(dataSource, sql);
    }

    public static Response response(Supplier<MycatResultSetResponse[]> response) {
        return JdbcLib.response(response);
    }

    public static Supplier<MycatResultSetResponse[]> queryJdbcByDataSource(String dataSource, String... sql) {
        return JdbcLib.queryJdbcByDataSource(dataSource, sql);
    }

    public static Response updateJdbcByDataSource(String dataSource, boolean needGeneratedKeys, String... sql) {
        return JdbcLib.responseUpdateOnJdbcByDataSource(dataSource, needGeneratedKeys, sql);
    }

    public static Response setTransactionIsolation(String text) {
        return JdbcLib.setTransactionIsolation(text);
    }

    public static Response setTransactionIsolation(int transactionIsolation) {
        return JdbcLib.setTransactionIsolation(transactionIsolation);
    }

    public static Response setAutocommit(boolean autocommit) {
        return JdbcLib.setAutocommit(autocommit);
    }
}