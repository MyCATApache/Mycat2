package io.mycat.proxy.command;

/**
 * @author jamie12221
 * @date 2019-05-12 21:00
 * mysql server 对LocalData 的处理
 **/
public interface LocalInFileRequestHandler {

  int COM_QUERY = 0;
  int LOCAL_INFILE_REQUEST = 1;
  int CONTENT_OF_FILE = 2;
  int EMPTY_PACKET = 3;

  void handleQuery(byte[] sql, MycatSessionView seesion);

  void handleContentOfFilename(byte[] sql, MycatSessionView seesion);

//  void writeLocalInFileRequestEndPacket(byte[] fileName,MycatSessionView seesion);

  void handleContentOfFilenameEmptyOk();

  interface LocalInFileSession {

    int getLocalInFileState();

    void setLocalInFileState(int value);
  }

}
