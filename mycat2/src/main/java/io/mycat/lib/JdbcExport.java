package io.mycat.lib;

import io.mycat.pattern.InstructionSet;
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

    public static Response updateOnJdbcByDataSource(String dataSource, String sql) {
        return updateOnJdbcByDataSource(dataSource, new String[]{sql});
    }

    public static Response updateOnJdbcByDataSource(String dataSource, String... sql) {
        return updateOnJdbcByDataSource(dataSource, false, sql);
    }

    public static Response updateOnJdbcByDataSource(String dataSource, boolean needGeneratedKeys, String... sql) {
        return JdbcLib.responseUpdateOnJdbcByDataSource(dataSource, needGeneratedKeys, sql);
    }

    public static Response responseOnJdbcSetTransactionIsolation(String text) {
        return JdbcLib.setTransactionIsolation(text);
    }

    public static Response responseOnJdbcSetTransactionIsolation(int transactionIsolation) {
        return JdbcLib.setTransactionIsolation(transactionIsolation);
    }

    public static Response responseOnJdbcSetAutocommit(boolean autocommit) {
        return JdbcLib.setAutocommit(autocommit);
    }
}