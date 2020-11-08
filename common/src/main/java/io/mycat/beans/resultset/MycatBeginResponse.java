package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatBeginResponse implements MycatResponse {
    final MycatResultSetType type = MycatResultSetType.BEGIN;
    public static final MycatBeginResponse INSTANCE = new MycatBeginResponse();

    @Override
    public MycatResultSetType getType() {
        return type;
    }

    @Override
    public void close() throws IOException {

    }
}