package io.mycat.lib.impl;

import io.mycat.MycatException;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.calcite.MetadataManager;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Supplier;

public enum  CalciteLib {
    INSTANCE;
    public Response responseQueryCalcite(String sql){
       return JdbcLib.response(queryCalcite(sql));
    }
    public Supplier<MycatResultSetResponse[]> queryCalcite(String sql) {
        return () -> {
            try {
                CalciteConnection connection = MetadataManager.INSATNCE.getConnection();
                Statement statement = null;
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
                return new MycatResultSetResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)};
            } catch (Exception e) {
                throw new MycatException(e);
            }
        };
    }
}