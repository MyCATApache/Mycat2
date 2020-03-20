package io.mycat.lib.impl;

import io.mycat.beans.resultset.MycatResultSetResponse;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;

import java.io.File;
import java.nio.file.Path;

@Log4j
public class CacheFile {
    public CacheFile(Path file, ResultSetCacheRecorder recorder, ResultSetCacheRecorder.Token token) {
        this.file = file;
        this.recorder = recorder;
        this.token = token;
    }

    Path file;
    ResultSetCacheRecorder recorder;
    ResultSetCacheRecorder.Token token;

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        close();
    }

    public void close() {
        try {
            File file = this.file.toFile();
            if (file.exists() && !file.delete()) {
                file.deleteOnExit();
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    @SneakyThrows
    public MycatResultSetResponse cacheResponse() {
        return recorder.newMycatResultSetResponse(token);
    }
}