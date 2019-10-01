package io.mycat.lib.impl;

import io.mycat.MycatException;
import io.mycat.beans.resultset.MycatResultSetResponse;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class CacheLib {

    final static ConcurrentHashMap<String, CacheFile> map = new ConcurrentHashMap<>();

    public static MycatResultSetResponse cacheResponse(String key, Supplier<MycatResultSetResponse> supplier) {
        CacheFile file = map.compute(key, (s, cacheFile) -> {
            if (cacheFile != null) {
                return cacheFile;
            }
            Path path = Paths.get(s).toAbsolutePath();
            try {
                Files.deleteIfExists(path);
                Files.createFile(path);
                String fileName = path.toString();
                ResultSetCacheRecorder resultSetCacheRecorder = new ResultSetCacheImpl(fileName);
                resultSetCacheRecorder.open();
                ByteBufferResponseRecorder responseRecorder = new ByteBufferResponseRecorder(resultSetCacheRecorder, supplier.get(), () -> {
                });
                responseRecorder.cache();
                ResultSetCacheRecorder.Token token = resultSetCacheRecorder.endRecord();
                return new CacheFile(path, resultSetCacheRecorder,token);
            } catch (IOException e) {
                throw new MycatException(e);
            }
        });
        try {
            return file.recorder.newMycatResultSetResponse(file.token);
        } catch (IOException e) {
            throw new MycatException(e);
        }
    }


    public static void removeCache(String key) {
        CacheFile remove = map.remove(key);
        if (remove != null) {
            Path file = remove.file;
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class CacheFile {
        public CacheFile(Path file, ResultSetCacheRecorder recorder, ResultSetCacheRecorder.Token token) {
            this.file = file;
            this.recorder = recorder;
            this.token = token;
        }
        Path file;
        ResultSetCacheRecorder recorder;
        ResultSetCacheRecorder.Token token;
    }
}