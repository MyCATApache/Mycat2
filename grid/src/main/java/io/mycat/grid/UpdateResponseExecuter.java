package io.mycat.grid;

import io.mycat.datasource.jdbc.MycatResponse;
import io.mycat.datasource.jdbc.MycatUpdateResponse;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;

public class UpdateResponseExecuter implements SQLExecuter {
 final MycatSession session;

  public UpdateResponseExecuter(MycatSession session) {
    this.session = session;
  }

  @Override
  public MycatResponse execute() throws Exception {
    return new MycatUpdateResponse() {
      @Override
      public void close() throws IOException {

      }

      @Override
      public int getUpdateCount() {
        return session.getAffectedRows();
      }

      @Override
      public long getLastInsertId() {
        return session.getLastInsertId();
      }

      @Override
      public int serverStatus() {
        return session.getServerStatus().getServerStatus();
      }
    };
  }

}