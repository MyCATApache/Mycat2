package io.mycat.grid;

import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetType;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;

public class UpdateResponse implements MycatResponse {
 final MycatSession session;

  public UpdateResponse(MycatSession session) {
    this.session = session;
  }

    @Override
    public void close() throws IOException {

    }


    public int getUpdateCount() {
        return session.getAffectedRows();
    }


    public long getLastInsertId() {
        return session.getLastInsertId();
    }


    public int serverStatus() {
        return session.getServerStatus().getServerStatus();
    }

    @Override
    public MycatResultSetType getType() {
        return MycatResultSetType.UPDATEOK;
    }
}