package io.mycat.util;


import com.alibaba.fastsql.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.MycatRowMetaDataImpl;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class SQL2ResultSetUtil {

    public static MycatRowMetaData getMycatRowMetaData(MySqlCreateTableStatement mySqlCreateTableStatement) {
        String tableName = mySqlCreateTableStatement.getTableSource().computeAlias();
        return new MycatRowMetaDataImpl(mySqlCreateTableStatement.getColumnDefinitions(), "", tableName);
    }
    @SneakyThrows
    public static MycatRowMetaData getMycatRowMetaData(JdbcConnectionManager jdbcConnectionManager,
                                                       String prototypeServer,
                                                       SQLCreateViewStatement mySqlCreateTableStatement) {
        try(DefaultConnection connection = jdbcConnectionManager.getConnection(prototypeServer)){
            Connection rawConnection = connection.getRawConnection();
            try(Statement statement = rawConnection.createStatement()){
                statement.setMaxRows(0);
                ResultSet resultSet = statement.executeQuery("select * from "+mySqlCreateTableStatement.getTableSource()+" where 0");
                resultSet.next();
                return new CopyMycatRowMetaData(new JdbcRowMetaData(resultSet.getMetaData()));
            }
        }
    }
}