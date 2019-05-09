package io.mycat.proxy;

import io.mycat.proxy.session.MycatSession;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-09 00:20
 **/
public interface MycatSessionWriteHandler {
  public void writeToChannel(MycatSession session) throws IOException ;
  public void onWriteFinished(MycatSession session) throws IOException ;
}
