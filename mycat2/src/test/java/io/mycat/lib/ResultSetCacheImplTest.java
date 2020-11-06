//package io.mycat.lib;
//
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import io.mycat.lib.impl.ResultSetCacheImpl;
//import io.mycat.lib.impl.ResultSetCacheRecorder;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.nio.ByteBuffer;
//import java.util.Iterator;
//
//public class ResultSetCacheImplTest {
//    @Test
//    public void test() throws Exception {
//        ResultSetCacheImpl resultSetCache = new ResultSetCacheImpl("s");
//        resultSetCache.open();
//        resultSetCache.startRecordColumn(1);
//        resultSetCache.addColumnDefBytes("1111".getBytes());
//        resultSetCache.startRecordRow();
//        resultSetCache.addRowBytes("2222".getBytes());
//        ResultSetCacheRecorder.Token token = resultSetCache.endRecord();
//        resultSetCache.sync();
//        resultSetCache.close();
//
//        resultSetCache.open();
//
//        MycatResultSetResponse response = resultSetCache.newMycatResultSetResponse(token);
//
//        Assert.assertEquals(1, response.columnCount());
//
//        Iterator<ByteBuffer> columnDefIterator = response.columnDefIterator();
//        ByteBuffer next = columnDefIterator.next();
//        Assert.assertEquals(ByteBuffer.wrap("1111".getBytes()), next);
//        Assert.assertFalse(columnDefIterator.hasNext());
//
//        Iterator<ByteBuffer> iterator = response.rowIterator();
//        Assert.assertEquals(ByteBuffer.wrap("2222".getBytes()), iterator.next());
//        Assert.assertFalse(iterator.hasNext());
//
//
//    }
//}