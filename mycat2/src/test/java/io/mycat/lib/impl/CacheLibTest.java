//package io.mycat.lib.impl;
//
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import io.mycat.lib.CacheResultSetExport;
//import io.mycat.DefResultSet;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.nio.charset.Charset;
//import java.nio.file.Files;
//import java.util.Iterator;
//
//public class CacheLibTest {
//    @Test
//    public void test() throws Exception {
//
//        String cacheLib = "s";
//        DefResultSet resultSet = new DefResultSet(1,33, Charset.defaultCharset());
//        resultSet.addColumnDef(0,"c1",1);
//        resultSet.addTextRowPayload("1");
//        MycatResultSetResponse response = CacheResultSetExport.cacheResponse(cacheLib, () -> resultSet);
//        MycatResultSetResponse response1 = CacheResultSetExport.cacheResponse(cacheLib, () -> null);
//        Assert.assertEquals(response.columnCount(),response1.columnCount());
//
//        Iterator columnDefIterator = response.columnDefIterator();
//        Iterator columnDefIterator1 = response1.columnDefIterator();
//
//        Iterator rowIterator = response.rowIterator();
//        Iterator rowIterator1 = response1.rowIterator();
//        Assert.assertEquals(rowIterator.next(),rowIterator1.next());
//
//        CacheResultSetExport.removeCache(cacheLib);
//
//
//    }
//    @Test
//    public void test1() throws Exception {
//
//        DefResultSet resultSet = new DefResultSet(1,33, Charset.defaultCharset());
//        resultSet.addColumnDef(0,"c1",1);
//        resultSet.addTextRowPayload("1");
//        TransforFileLib.saveToFile(Files.createTempFile("1","2").toString(),true,resultSet);
//
//
//    }
//}