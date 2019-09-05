package io.mycat.grid;

import io.mycat.beans.resultset.SQLExecuter;
import io.mycat.calcite.MetadataManager;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.proxy.session.MycatSession;
import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CalciteExecuterBuilderImpl implements ExecuterBuilder {

    private final MycatSession session;
    private final GRuntime jdbcRuntime;

    public CalciteExecuterBuilderImpl(MycatSession session, GRuntime jdbcRuntime) {

        this.session = session;
        this.jdbcRuntime = jdbcRuntime;
    }

    @Override
    public SQLExecuter[] generate(byte[] sqlBytes) {
        CalciteConnection connection = MetadataManager.INSATNCE.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(new String(sqlBytes));
            JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
            return new SQLExecuter[]{() -> new TextResultSetResponse(jdbcRowBaseIterator)};
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}