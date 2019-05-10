package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;

/**
 * @author jamie12221
 * @date 2019-05-10 22:24
 **/
public class LoadDataUtil {

  public static void loadData(MySQLClientSession mysql, String sql,
      AsynTaskCallBack<MySQLClientSession> callback) {
    new LoadDataRequestTask()
        .request(mysql, 3, sql, new AsynTaskCallBack<MySQLClientSession>() {
          @Override
          public void finished(MySQLClientSession session, Object sender, boolean success,
              Object result,
              Object attr) {
            try {
              FileChannel file = FileChannel.open(Paths.get((String) result));
              loadDataFileContext(session, file, 0, (int) file.size(),
                  new AsynTaskCallBack<MySQLClientSession>() {
                    @Override
                    public void finished(MySQLClientSession session, Object sender, boolean success,
                        Object packetId, Object attr) {
                      if (success) {
                        loadDataEmptyPacket(session, callback, session.incrementPacketIdAndGet());
                      } else {
                        callback.finished(session, this, false, null, attr);
                      }
                    }
                  });
            } catch (Exception e) {

              callback.finished(session, this, false, null, attr);
            }
          }
        });

  }

  private static void loadDataFileContext(MySQLClientSession mysql, FileChannel fileChannel,
      int position, int length,
      AsynTaskCallBack<MySQLClientSession> callback) throws Exception {
    new FileChannelPayloadWriterHandler()
        .request(mysql, fileChannel,
            position, length, callback);
  }

  private static void loadDataEmptyPacket(MySQLClientSession mysql,
      AsynTaskCallBack<MySQLClientSession> callback,
      byte nextPacketId) {
    new CommandTask().requestEmptyPacket(mysql, nextPacketId, callback);
  }

  private static class LoadDataRequestTask implements ResultSetTask {

    String fileName;

    @Override
    public void onFinished(MySQLClientSession mysql, boolean success, String errorMessage) {
      AsynTaskCallBack<MySQLClientSession> callBack = mysql.getCallBackAndReset();
      callBack.finished(mysql, this, success, fileName, errorMessage);
    }

    @Override
    public void onLoadDataRequest(MySQLPacket mySQLPacket, int startPos, int endPos) {
      fileName = mySQLPacket.getEOFString(startPos + 1);
      clearAndFinished(getSessionCaller(), true, null);
    }
  }

  private static class FileChannelPayloadWriterHandler extends AbstractPayloadWriter<FileChannel> {

    @Override
    protected int writePayload(FileChannel fileChannel, int writeIndex, int reminsPacketLen,
        SocketChannel serverSocket) throws IOException {
      return (int) fileChannel.transferTo(writeIndex, reminsPacketLen, serverSocket);
    }

    @Override
    void clearResource(FileChannel f) throws Exception {
      f.close();
    }
  }
}
