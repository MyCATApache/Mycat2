package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatRollbackResponse implements MycatResponse {
    final  MycatResultSetType type = MycatResultSetType.ROLLBACK;
    public static final MycatRollbackResponse INSTANCE = new MycatRollbackResponse();

    @Override
    public MycatResultSetType getType() {
        return type;
    }

    @Override
    public void close() throws IOException {

    }
}