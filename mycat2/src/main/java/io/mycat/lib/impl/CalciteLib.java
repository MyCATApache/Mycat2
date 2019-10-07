package io.mycat.lib.impl;

import io.mycat.MycatException;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.calcite.MetadataManager;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.util.MycatRowMetaDataImpl;
import io.mycat.util.SQL2ResultSetUtil;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;

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
    public Supplier<MycatResultSetResponse[]> queryCalcite(String columnSQL,RelNode rootRel) {
        return () -> {
            try {
                CalciteConnection connection = MetadataManager.INSATNCE.getConnection();
                Statement statement = null;
                statement = connection.createStatement();
                CalcitePrepare.Context prepareContext = connection.createPrepareContext();
                DataContext dataContext =prepareContext.getDataContext();
                final Interpreter interpreter = new Interpreter(dataContext, rootRel);
                MycatRowMetaDataImpl mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData(columnSQL);
                Enumerator<Object[]> enumerator = interpreter.enumerator();
                EnumeratorRowIterator enumeratorRowIterator = new EnumeratorRowIterator(mycatRowMetaData,enumerator);
                return new MycatResultSetResponse[]{new TextResultSetResponse(enumeratorRowIterator)};
            } catch (Exception e) {
                throw new MycatException(e);
            }
        };
    }
}