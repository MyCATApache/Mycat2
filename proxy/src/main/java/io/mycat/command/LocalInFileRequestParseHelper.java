package io.mycat.command;


import io.mycat.proxy.session.MycatSession;

/**
 * @author jamie12221
 *  date 2019-05-12 21:00
 * mysql server 对LocalData 的处理
 **/
public interface LocalInFileRequestParseHelper {

  void handleQuery(byte[] sql, MycatSession seesion);

  void handleContentOfFilename(byte[] sql, MycatSession session);

  void handleContentOfFilenameEmptyOk(MycatSession session);

  interface LocalInFileSession {

    boolean shouldHandleContentOfFilename();

    void setHandleContentOfFilename(boolean need);
  }

}
