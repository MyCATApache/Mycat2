//package io.mycat.lib.impl;
//
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import lombok.SneakyThrows;
//
//public class CacheFile {
//    public CacheFile(ResultSetCacheRecorder recorder, ResultSetCacheRecorder.Token token) {
//        this.recorder = recorder;
//        this.token = token;
//    }
//    ResultSetCacheRecorder recorder;
//    ResultSetCacheRecorder.Token token;
//
//    @Override
//    public void finalize() throws Throwable {
//        super.finalize();
//        close();
//    }
//
//    public void close() {
//        try {
//            recorder.close();
//        } catch (Exception e) {
//           // log.error(e);
//        }
//    }
//
//    @SneakyThrows
//    public MycatResultSetResponse cacheResponse() {
//        return recorder.newMycatResultSetResponse(token);
//    }
//}