package io.mycat.sql;

import io.mycat.assemble.MycatTest;
import io.vertx.mysqlclient.MySQLPool;
import org.apache.curator.shaded.com.google.common.base.Objects;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.*;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class ColumnDefTest implements MycatTest {
    @Test
    public void testSelectDual() throws Exception {
        try (Connection mycatConnection = getMySQLConnection(DB_MYCAT);
             Connection mysqlConnection = getMySQLConnection(DB1)) {
            execute(mycatConnection, RESET_CONFIG);
            String sql = "SELECT ROW_COUNT()";
            ResultSetMetaData left = getResultSetMetaData(mycatConnection, sql);
            ResultSetMetaData right = getResultSetMetaData(mysqlConnection, sql);
            assertEquals(left, right);
        }
    }

    private boolean equals(ResultSetMetaData left, ResultSetMetaData right) throws SQLException {
        if (left.getColumnCount() != right.getColumnCount()) {
            return false;
        }
        int columnCount = left.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            if (Objects.equal(left.getColumnName(i), right.getColumnName(i))
                    &&
                    Objects.equal(left.isAutoIncrement(i), right.isAutoIncrement(i))
                    &&
                    Objects.equal(left.isCaseSensitive(i), right.isCaseSensitive(i))
                    &&
                    Objects.equal(left.isSearchable(i), right.isSearchable(i))
                    &&
                    Objects.equal(left.isCurrency(i), right.isCurrency(i))
                    &&
                    Objects.equal(left.isNullable(i), right.isNullable(i))
                    &&
                    Objects.equal(left.isSigned(i), right.isSigned(i))
                    &&
                    Objects.equal(left.getColumnDisplaySize(i), right.getColumnDisplaySize(i))
                    &&
                    Objects.equal(left.getColumnLabel(i), right.getColumnLabel(i))
                    &&
                    Objects.equal(left.getColumnName(i), right.getColumnName(i))
                    &&
                    Objects.equal(left.getSchemaName(i), right.getSchemaName(i))
                    &&
                    Objects.equal(left.getPrecision(i), right.getPrecision(i))
                    &&
                    Objects.equal(left.getScale(i), right.getScale(i))
                    &&
                    Objects.equal(left.getTableName(i), right.getTableName(i))
                    &&
                    Objects.equal(left.getCatalogName(i), right.getCatalogName(i))
                    &&
                    Objects.equal(left.getColumnType(i), right.getColumnType(i))
                    &&
                    Objects.equal(left.getColumnTypeName(i), right.getColumnTypeName(i))
                    &&
                    Objects.equal(left.isReadOnly(i), right.isReadOnly(i))
                    &&
                    Objects.equal(left.isWritable(i), right.isWritable(i))
                    &&
                    Objects.equal(left.isDefinitelyWritable(i), right.isDefinitelyWritable(i))
                    &&
                    Objects.equal(left.getColumnClassName(i), right.getColumnClassName(i))) {
                continue;
            } else {
                System.out.println();
                return false;
            }
        }
        return true;
    }

    private void assertEquals(ResultSetMetaData left, ResultSetMetaData right) throws SQLException {
        Assert.assertEquals(left.getColumnCount(), right.getColumnCount());
        int columnCount = left.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            Assert.assertEquals(left.getColumnName(i), right.getColumnName(i));
            Assert.assertEquals(left.isAutoIncrement(i), right.isAutoIncrement(i));
            Assert.assertEquals(left.isCaseSensitive(i), right.isCaseSensitive(i));
            Assert.assertEquals(left.isSearchable(i), right.isSearchable(i));
            Assert.assertEquals(left.isCurrency(i), right.isCurrency(i));
//            Assert.assertEquals(left.isNullable(i), right.isNullable(i));
            Assert.assertEquals(left.isSigned(i), right.isSigned(i));
//            Assert.assertEquals(left.getColumnDisplaySize(i), right.getColumnDisplaySize(i));
            Assert.assertEquals(left.getColumnLabel(i), right.getColumnLabel(i));
            Assert.assertEquals(left.getColumnName(i), right.getColumnName(i));
            Assert.assertEquals(left.getSchemaName(i), right.getSchemaName(i));
            Assert.assertEquals(left.getPrecision(i), right.getPrecision(i));
            Assert.assertEquals(left.getScale(i), right.getScale(i));
            Assert.assertEquals(left.getTableName(i), right.getTableName(i));
            Assert.assertEquals(left.getCatalogName(i), right.getCatalogName(i));
            Assert.assertEquals(left.getColumnType(i), right.getColumnType(i));
            Assert.assertEquals(left.getColumnTypeName(i), right.getColumnTypeName(i));
//            Assert.assertEquals(left.isReadOnly(i), right.isReadOnly(i));
//            Assert.assertEquals(left.isWritable(i), right.isWritable(i));
//            Assert.assertEquals(left.isDefinitelyWritable(i), right.isDefinitelyWritable(i));
            Assert.assertEquals(left.getColumnClassName(i), right.getColumnClassName(i));
        }
    }

    private ResultSetMetaData getResultSetMetaData(Connection mycatConnection, String sql) throws SQLException {
        Statement statement = mycatConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();
        return metaData;
    }
}