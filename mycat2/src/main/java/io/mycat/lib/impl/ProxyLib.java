package io.mycat.lib.impl;

import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.proxy.SQLExecuterWriter;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyLib {
    public final static Response responseOk = (session, matcher) -> session.writeOkEndPacket();
    public static ResultSetCacheImpl resultSetCache;
    public final static ConcurrentHashMap<String, ResultSetCacheRecorder.Token> cache = new ConcurrentHashMap<>();

    public static Response proxyQueryOnDatasource(String dataSource,String sql) {
//        return (session, matcher) -> MySQLTaskUtil.proxyBackend(session,sql,dataSource);

        return null;
    }


    public static Response useSchemaThenResponseOk(String schema) {
        return (session, matcher) -> {
            matcher.getTableCollector().useSchema(schema);
            session.writeOkEndPacket();
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
                cache.put(fileName, resultSetCache.endRecord());
            }
        });
        return getResponse(byteBufferResponseRecorder);
    }

    private static Response getResponse(MycatResultSetResponse response) {
        return (session, matcher) -> SQLExecuterWriter.writeToMycatSession(session, response);
    }

    public static Response setTransactionIsolationThenResponseOk(String text) {
        return (session, matcher) -> {
            session.setIsolation(MySQLIsolation.valueOf(text));
            session.writeOkEndPacket();
        };
    }
}