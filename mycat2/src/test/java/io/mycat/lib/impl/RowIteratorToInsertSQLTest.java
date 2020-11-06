package io.mycat.lib.impl;

import io.mycat.router.util.InserParser;
import io.mycat.router.util.RowIteratorToInsertSQL;
import org.junit.Assert;
import org.junit.Test;

/**
 * junwen 294712221@qq.com
 */
public class RowIteratorToInsertSQLTest {
    @Test
    public void test() throws Exception {
        RowIteratorToInsertSQL iteratorToInsertSQL = new RowIteratorToInsertSQL("test", new InserParser("/sql/show_databases.sql"), 1);
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('db1');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('db2');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('db3');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('information_schema');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('mysql');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('performance_schema');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('test');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('test_db');",iteratorToInsertSQL.next());
        Assert.assertTrue(!iteratorToInsertSQL.hasNext());
    }
    @Test
    public void test2() throws Exception {
        RowIteratorToInsertSQL iteratorToInsertSQL = new RowIteratorToInsertSQL("test", new InserParser("/sql/show_databases.sql"), 3);
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('db1'),('db2'),('db3');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('information_schema'),('mysql'),('performance_schema');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('test'),('test_db');",iteratorToInsertSQL.next());
        Assert.assertTrue(!iteratorToInsertSQL.hasNext());
    }
    @Test
    public void test3() throws Exception {
        RowIteratorToInsertSQL iteratorToInsertSQL = new RowIteratorToInsertSQL("test", new InserParser("/sql/show_databases.sql"), 4);
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('db1'),('db2'),('db3'),('information_schema');",iteratorToInsertSQL.next());
        Assert.assertTrue(iteratorToInsertSQL.hasNext());
        Assert.assertEquals("INSERT INTO test (`Database`) VALUES ('mysql'),('performance_schema'),('test'),('test_db');",iteratorToInsertSQL.next());
        Assert.assertTrue(!iteratorToInsertSQL.hasNext());
    }
}