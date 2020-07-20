package io.mycat.testsuite.tools;

import com.alibaba.druid.mock.MockStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestConnection extends com.alibaba.druid.mock.MockConnection  {
    private SimpleConnection simpleConnection;

    public TestConnection(SimpleConnection simpleConnection) {
        this.simpleConnection = simpleConnection;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        simpleConnection.useSchema(schema);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new MockStatement(this){
            @Override
            public ResultSet executeQuery(String sql) throws SQLException {
                SimpleConnection.ResultSet resultSet = simpleConnection.executeQuery(sql);
                return new TestMockResultSet (this,resultSet.getColumnList(),resultSet.getRows());
            }
        };
    }
}