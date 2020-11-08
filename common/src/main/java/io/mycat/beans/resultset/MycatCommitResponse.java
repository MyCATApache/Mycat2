package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatCommitResponse implements MycatResponse {
    final MycatResultSetType type = MycatResultSetType.COMMIT;
    public static final MycatCommitResponse INSTANCE = new MycatCommitResponse();

    @Override
    public MycatResultSetType getType() {
        return type;
    }

    @Override
    public void close() throws IOException {

    }
}