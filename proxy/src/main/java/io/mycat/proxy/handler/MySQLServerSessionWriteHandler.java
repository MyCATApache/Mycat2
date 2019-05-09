package io.mycat.proxy.handler;

import io.mycat.proxy.session.MycatSession;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-08 10:09
 **/
public class MySQLServerSessionWriteHandler implements MycatSessionWriteHandler {

  @Override
  public void onSocketWrite(MycatSession session) throws IOException {

  }

  @Override
  public void onWriteFinished(MycatSession session) throws IOException {

  }

}
