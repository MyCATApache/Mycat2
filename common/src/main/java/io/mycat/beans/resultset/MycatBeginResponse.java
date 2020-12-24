package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatBeginResponse implements MycatResponse {
    public static final MycatBeginResponse INSTANCE = new MycatBeginResponse();
    final MycatResultSetType type = MycatResultSetType.BEGIN;

    @Override
    public MycatResultSetType getType() {
        return type;
    }

    @Override
    public void close() throws IOException {

    }
}