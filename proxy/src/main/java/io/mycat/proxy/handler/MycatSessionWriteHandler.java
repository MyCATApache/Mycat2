package io.mycat.proxy.handler;

import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-07 23:50
 **/
public interface MycatSessionWriteHandler {

  void onSocketWrite(MycatSession mycat) throws IOException;

  void onWriteFinished(MycatSession mycat) throws IOException;
}
