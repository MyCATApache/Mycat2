package io.mycat.lib.impl;

import io.mycat.MycatException;
import io.mycat.SQLExecuterWriter;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.proxy.session.MycatSession;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.router.util.InserParser;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author chen junwen
 */
public class FinalCacheLib {
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
        byteBufferResponseRecorder.cache();
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
            public void apply(MycatSession session) {
                SQLExecuterWriter.writeToMycatSession(session, response);
            }
        };
    }
}