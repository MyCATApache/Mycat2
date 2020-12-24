package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatRollbackResponse implements MycatResponse {
    public static final MycatRollbackResponse INSTANCE = new MycatRollbackResponse();
    final MycatResultSetType type = MycatResultSetType.ROLLBACK;

    @Override
    public MycatResultSetType getType() {
        return type;
    }

    @Override
    public void close() throws IOException {

    }
}