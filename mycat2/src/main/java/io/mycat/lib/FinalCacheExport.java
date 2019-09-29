package io.mycat.lib;

import cn.lightfish.pattern.DynamicSQLMatcher;
import cn.lightfish.pattern.InstructionSet;
import io.mycat.MycatException;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.SQLExecuter;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.proxy.SQLExecuterWriter;
import io.mycat.proxy.session.MycatSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class FinalCacheExport implements InstructionSet {

    public static Response responseFinalCache(String key) {
        return Lib.responseFinalCache(key);
    }

    public static void finalCacheFile(String fileName) {
        Lib.finalCacheFile(fileName);
    }
    public static void initFinalCacheFile(String cachePath){
        Lib.initFinalCacheFile(cachePath);
    }
    public static class Lib {
        private static ResultSetCacheImpl resultSetCache;
        private final static HashMap<String, ResultSetCacheRecorder.Token> cache = new HashMap<>();

        public static void initFinalCacheFile(String cachePath) {
            try {
                resultSetCache = new ResultSetCacheImpl(cachePath);
                resultSetCache.open();
            } catch (IOException e) {
                throw new MycatException(e);
            }
        }

        public static void finalCacheFile(String fileName) {
            InserParser inserParser = new InserParser(fileName);
            ByteBufferResponseRecorder byteBufferResponseRecorder = new ByteBufferResponseRecorder(resultSetCache, new TextResultSetResponse(inserParser), new Runnable() {
                @Override
                public void run() {
                    cache.put(fileName, resultSetCache.endRecord());
                }
            });

            byteBufferResponseRecorder.columnCount();
            Iterator<byte[]> iterator = byteBufferResponseRecorder.columnDefIterator();
            while (iterator.hasNext()) {
                iterator.next();
            }
            Iterator<byte[]> iterator1 = byteBufferResponseRecorder.rowIterator();
            while (iterator1.hasNext()) {
                iterator1.next();
            }
            try {
                byteBufferResponseRecorder.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static Response responseFinalCache(String fileName) {
            ResultSetCacheRecorder.Token token = cache.get(fileName);
            if (token != null) {
                try {
                    MycatResultSetResponse response = resultSetCache.newMycatResultSetResponse(token);
                    return getResponse(response);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            throw new MycatException("no file");
        }

        private static Response getResponse(MycatResultSetResponse response) {
            return new Response() {
                @Override
                public void apply(MycatSession session, DynamicSQLMatcher matcher) {
                    SQLExecuterWriter.writeToMycatSession(session, new SQLExecuter() {
                        @Override
                        public MycatResponse execute() throws Exception {
                            return response;
                        }
                    });
                }
            };
        }

    }
}