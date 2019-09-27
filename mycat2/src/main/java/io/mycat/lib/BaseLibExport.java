package io.mycat.lib;

import cn.lightfish.pattern.DynamicSQLMatcher;
import cn.lightfish.pattern.InstructionSet;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.SQLExecuter;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.proxy.SQLExecuterWriter;
import io.mycat.proxy.session.MycatSession;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class BaseLibExport implements InstructionSet {
    public static Response useSchemaThenResponseOk(String schema) {
        return Lib.useSchemaThenResponseOk(schema);
    }

    public static Response cacheLocalFileThenResponse(String fileName) {
        return Lib.cacheLocalFileThenResponse(fileName);
    }

    public static Response responseOk() {
        return Lib.responseOk;
    }

    public static Response useSchemaThenResponseOk() {
        return Lib.responseOk;
    }

    public static class Lib {
        public final static Response responseOk = (session, matcher) -> session.writeOkEndPacket();
        public final static ResultSetCacheImpl resultSetCache = new ResultSetCacheImpl("d:/baseCache");
        public final static ConcurrentHashMap<String, ResultSetCacheRecorder.Token> cache = new ConcurrentHashMap<>();

        public static Response useSchemaThenResponseOk(String schema) {
            return new Response() {
                @Override
                public void apply(MycatSession session, DynamicSQLMatcher matcher) {
                    matcher.getTableCollector().useSchema(schema);
                    session.writeOkEndPacket();
                }
            };
        }

        public static Response cacheLocalFileThenResponse(String fileName) {
            ResultSetCacheRecorder.Token token = cache.get(fileName);
            if (token != null) {
                try {
                    MycatResultSetResponse response = resultSetCache.newMycatResultSetResponse(token);
                    return getResponse(response);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                resultSetCache.open();
            } catch (IOException e) {
                e.printStackTrace();
            }
            InserParser inserParser = new InserParser(fileName);
            ByteBufferResponseRecorder byteBufferResponseRecorder = new ByteBufferResponseRecorder(resultSetCache, new TextResultSetResponse(inserParser), new Runnable() {
                @Override
                public void run() {
                    cache.put(fileName,resultSetCache.endRecord());
                }
            });
            return getResponse(byteBufferResponseRecorder);
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