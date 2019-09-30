package io.mycat.lib.impl;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class CacheLib {

    final static ConcurrentHashMap<String, CacheFile> map = new ConcurrentHashMap<>();

    public static Supplier<MycatResultSetResponse[]> cacheResponse(String key, Supplier<MycatResultSetResponse[]> supplier) {
       return null;
    }

    public static void removeCache(String key) {
        CacheFile remove = map.remove(key);
        if (remove != null) {
            File file = remove.file;
            if (!file.delete()) {
                file.deleteOnExit();
            }
        }
    }
    static class CacheFile{
        File file;
        MappedByteBuffer byteBuffer;
    }
}