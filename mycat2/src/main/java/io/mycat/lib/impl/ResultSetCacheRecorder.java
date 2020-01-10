package io.mycat.lib.impl;

import io.mycat.beans.resultset.MycatResultSetResponse;

import java.io.IOException;

/**
 * @author chen junwen
 */
public interface ResultSetCacheRecorder {
    void open() throws IOException;

    void sync() throws IOException;

    void close() throws IOException;

    void startRecordColumn(int columnCount);

    void addColumnDefBytes(byte[] bytes);

    void startRecordRow() ;

    void addRowBytes(byte[] bytes) ;

    Token endRecord();

    public MycatResultSetResponse newMycatResultSetResponse(Token token) throws IOException;

    public interface Token{

    }
}