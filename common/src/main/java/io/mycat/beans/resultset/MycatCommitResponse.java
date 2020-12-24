package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatCommitResponse implements MycatResponse {
    public static final MycatCommitResponse INSTANCE = new MycatCommitResponse();
    final MycatResultSetType type = MycatResultSetType.COMMIT;

    @Override
    public MycatResultSetType getType() {
        return type;
    }

    @Override
    public void close() throws IOException {

    }
}