package io.mycat.lib.impl;

import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.lib.CacheExport;
import io.mycat.proxy.DefResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

public class CacheLibTest {
    @Test
    public void test() throws Exception {

        String cacheLib = Files.createTempFile("CacheLib", Integer.valueOf(ThreadLocalRandom.current().nextInt(0, 100))
                .toString()).toAbsolutePath().toString();
        DefResultSet resultSet = new DefResultSet(1,33, Charset.defaultCharset());
        resultSet.addColumnDef(0,"c1",1);
        resultSet.addTextRowPayload("1");
        MycatResultSetResponse response = CacheExport.cacheResponse(cacheLib, () -> resultSet);
        MycatResultSetResponse response1 = CacheExport.cacheResponse(cacheLib, () -> null);
        Assert.assertEquals(response.columnCount(),response1.columnCount());

        Iterator columnDefIterator = response.columnDefIterator();
        Iterator columnDefIterator1 = response1.columnDefIterator();

        Iterator rowIterator = response.rowIterator();
        Iterator rowIterator1 = response1.rowIterator();
        Assert.assertEquals(rowIterator.next(),rowIterator1.next());


    }
}