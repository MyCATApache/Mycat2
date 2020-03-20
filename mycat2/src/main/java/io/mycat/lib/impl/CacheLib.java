package io.mycat.lib.impl;

import io.mycat.MycatException;
import io.mycat.beans.resultset.MycatResultSetResponse;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author chen junwen
 */
public class CacheLib {

    final static ConcurrentHashMap<String, CacheFile> map = new ConcurrentHashMap<>();

    public static MycatResultSetResponse cacheResponse(String key, Supplier<MycatResultSetResponse> supplier) {
        CacheFile file = map.compute(key, cacheFileFromResponse(supplier));
        try {
            return file.recorder.newMycatResultSetResponse(file.token);
        } catch (IOException e) {
            throw new MycatException(e);
        }
    }

    @NotNull
    public static BiFunction<String, CacheFile, CacheFile> cacheFileFromResponse(Supplier<MycatResultSetResponse> supplier) {
        return (s, cacheFile) -> {
            if (cacheFile != null) {
                return cacheFile;
            }
            return cache(supplier, s);
        };
    }

    @NotNull
    @SneakyThrows
    public static CacheFile cache(Supplier<MycatResultSetResponse> supplier, String cacheFileName) {
//        Path path = Paths.get(cacheFileName).toAbsolutePath();
//        Files.deleteIfExists(path);
//        Files.createFile(path);
//        String fileName = path.toString();
        ResultSetCacheImpl resultSetCacheRecorder = new ResultSetCacheImpl(cacheFileName);
        resultSetCacheRecorder.open();
        ByteBufferResponseRecorder responseRecorder = new ByteBufferResponseRecorder(resultSetCacheRecorder, supplier.get(), () -> {
        });
        responseRecorder.cache();
        ResultSetCacheRecorder.Token token = resultSetCacheRecorder.endRecord();
        return new CacheFile(resultSetCacheRecorder.getFlie().toPath(), resultSetCacheRecorder, token);
    }


    public static void removeCache(String key) {
        CacheFile remove = map.remove(key);
        if (remove != null) {
            remove.close();
        }
    }
}
