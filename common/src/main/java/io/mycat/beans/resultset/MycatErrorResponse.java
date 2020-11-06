package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatErrorResponse implements MycatResponse {
    public static final MycatErrorResponse INSTANCE = new MycatErrorResponse();
    @Override
    public MycatResultSetType getType() {
        return MycatResultSetType.ERROR;
    }

    @Override
    public void close() throws IOException {

    }
}