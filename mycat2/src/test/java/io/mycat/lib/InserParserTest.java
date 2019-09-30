package io.mycat.lib;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.lib.impl.InserParser;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.JDBCType;

public class InserParserTest {

    /**
     * varChar
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        InserParser inserParser = new InserParser("/sql/show_databases.sql");
        MycatRowMetaData rowMetaData = inserParser.metaData();
        Assert.assertEquals("SCHEMATA", rowMetaData.getTableName(1));
        Assert.assertEquals(1, rowMetaData.getColumnCount());
        Assert.assertEquals(JDBCType.VARCHAR, JDBCType.valueOf(rowMetaData.getColumnType(1)));
        inserParser.next();
        Assert.assertEquals("db1",inserParser.getString(1));
        inserParser.next();
        Assert.assertEquals("db2",inserParser.getString(1));
        inserParser.next();
        Assert.assertEquals("db3",inserParser.getString(1));
        inserParser.next();
        Assert.assertEquals("information_schema",inserParser.getString(1));
        inserParser.next();
        Assert.assertEquals("mysql",inserParser.getString(1));
        inserParser.next();
        Assert.assertEquals("performance_schema",inserParser.getString(1));
        inserParser.next();
        Assert.assertEquals("test",inserParser.getString(1));
        inserParser.next();
        Assert.assertEquals("test_db",inserParser.getString(1));
        Assert.assertFalse(inserParser.next());
    }

    @Test
    public void testEmptyRow() throws Exception {
        InserParser inserParser = new InserParser("/sql/show_databases_empty.sql");
        MycatRowMetaData rowMetaData = inserParser.metaData();
        Assert.assertEquals("SCHEMATA", rowMetaData.getTableName(1));
        Assert.assertEquals(1, rowMetaData.getColumnCount());
        Assert.assertEquals(JDBCType.VARCHAR, JDBCType.valueOf(rowMetaData.getColumnType(1)));
        Assert.assertFalse(inserParser.next());
    }

    @Test
    public void test2() throws Exception {
        InserParser inserParser = new InserParser("/sql/travelrecord.sql");
        MycatRowMetaData rowMetaData = inserParser.metaData();
        Assert.assertEquals("travelrecord", rowMetaData.getTableName(1));
        Assert.assertEquals(7, rowMetaData.getColumnCount());
        Assert.assertEquals(JDBCType.BIGINT, JDBCType.valueOf(rowMetaData.getColumnType(1)));
        Assert.assertEquals(JDBCType.VARCHAR, JDBCType.valueOf(rowMetaData.getColumnType(2)));
        Assert.assertEquals(JDBCType.DATE, JDBCType.valueOf(rowMetaData.getColumnType(3)));
        Assert.assertEquals(JDBCType.DECIMAL, JDBCType.valueOf(rowMetaData.getColumnType(4)));
        Assert.assertEquals(JDBCType.INTEGER, JDBCType.valueOf(rowMetaData.getColumnType(5)));
        Assert.assertEquals(JDBCType.BLOB, JDBCType.valueOf(rowMetaData.getColumnType(6)));
        Assert.assertEquals(JDBCType.DOUBLE, JDBCType.valueOf(rowMetaData.getColumnType(7)));
        inserParser.next();
        Assert.assertEquals("1",inserParser.getString(1));
        Assert.assertEquals("1",inserParser.getString(2));
        Assert.assertEquals("2019-09-12",inserParser.getString(3));
        Assert.assertEquals(java.sql.Date.valueOf("2019-09-12"), inserParser.getDate(3));

        Assert.assertEquals("222",inserParser.getString(4));
        Assert.assertEquals(new BigDecimal("222"), inserParser.getBigDecimal(4));

        Assert.assertEquals("9",inserParser.getString(5));
        Assert.assertEquals(9,inserParser.getInt(5));
        Assert.assertEquals("ssss",inserParser.getString(6));
        Assert.assertArrayEquals("ssss".getBytes(),inserParser.getBytes(6));
        Assert.assertEquals("666.666",inserParser.getString(7));
        Assert.assertEquals(666.666, inserParser.getDouble(7), 0.0);
        inserParser.next();
        Assert.assertEquals("2",inserParser.getString(1));
        Assert.assertEquals(null,inserParser.getString(2));
        Assert.assertFalse(inserParser.next());
    }

    @Test
    public void test3() throws Exception {
        InserParser inserParser = new InserParser("/sql/travelrecord2.sql");
        MycatRowMetaData rowMetaData = inserParser.metaData();
        Assert.assertEquals("travelrecord", rowMetaData.getTableName(1));
        Assert.assertEquals(7, rowMetaData.getColumnCount());
        Assert.assertEquals(JDBCType.BIGINT, JDBCType.valueOf(rowMetaData.getColumnType(1)));
        Assert.assertEquals(JDBCType.VARCHAR, JDBCType.valueOf(rowMetaData.getColumnType(2)));
        Assert.assertEquals(JDBCType.DATE, JDBCType.valueOf(rowMetaData.getColumnType(3)));
        Assert.assertEquals(JDBCType.DECIMAL, JDBCType.valueOf(rowMetaData.getColumnType(4)));
        Assert.assertEquals(JDBCType.INTEGER, JDBCType.valueOf(rowMetaData.getColumnType(5)));
        Assert.assertEquals(JDBCType.BLOB, JDBCType.valueOf(rowMetaData.getColumnType(6)));
        Assert.assertEquals(JDBCType.DOUBLE, JDBCType.valueOf(rowMetaData.getColumnType(7)));
        inserParser.next();
        Assert.assertEquals("1",inserParser.getString(1));
        Assert.assertEquals("1",inserParser.getString(2));
        Assert.assertEquals("2019-09-12",inserParser.getString(3));
        Assert.assertEquals(java.sql.Date.valueOf("2019-09-12"), inserParser.getDate(3));

        Assert.assertEquals("222",inserParser.getString(4));
        Assert.assertEquals(new BigDecimal("222"), inserParser.getBigDecimal(4));

        Assert.assertEquals("9",inserParser.getString(5));
        Assert.assertEquals(9,inserParser.getInt(5));
        Assert.assertEquals("ssss",inserParser.getString(6));
        Assert.assertArrayEquals("ssss".getBytes(),inserParser.getBytes(6));
        Assert.assertEquals("666.666",inserParser.getString(7));
        Assert.assertEquals(666.666, inserParser.getDouble(7), 0.0);
        inserParser.next();
        Assert.assertEquals("2",inserParser.getString(1));
        Assert.assertEquals(null,inserParser.getString(2));
        inserParser.next();
        Assert.assertEquals("3",inserParser.getString(1));
        Assert.assertEquals(null,inserParser.getString(2));
        Assert.assertFalse(inserParser.next());
    }
}