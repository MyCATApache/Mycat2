package io.mycat.proxy.task.client;

import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.resultset.ResultSetTask;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;

/**
 * @author jamie12221
 * @date 2019-05-10 22:24
 * LoadData工具类
 **/
public class LoadDataUtil {

  /**
   * @param sql sql中的文件一定要存在的
   */
  public static void loadData(MySQLClientSession mysql, String sql,
      AsyncTaskCallBack<MySQLClientSession> callback) {
    new LoadDataRequestTask()
        .request(mysql, 3, sql, new AsyncTaskCallBack<MySQLClientSession>() {
          @Override
          public void finished(MySQLClientSession session, Object sender, boolean success,
              Object result,
              Object attr) {
            try {
              FileChannel file = FileChannel.open(Paths.get((String) result));
              loadDataFileContext(session, file, 0, (int) file.size(),
                  new AsyncTaskCallBack<MySQLClientSession>() {
                    @Override
                    public void finished(MySQLClientSession session, Object sender, boolean success,
                        Object packetId, Object attr) {
                      if (success) {
                        loadDataEmptyPacket(session, callback, session.incrementPacketIdAndGet());
                      } else {
                        callback.finished(session, this, false, packetId, null);
                      }
                    }
                  });
            } catch (Exception e) {
              String message = session.setLastMessage(e);
              callback.finished(session, this, false, message, attr);
            }
          }
        });

  }

  private static void loadDataFileContext(MySQLClientSession mysql, FileChannel fileChannel,
      int position, int length,
      AsyncTaskCallBack<MySQLClientSession> callback) throws Exception {
    new FileChannelPayloadWriterHandler()
        .request(mysql, fileChannel,
            position, length, callback);
  }

  private static void loadDataEmptyPacket(MySQLClientSession mysql,
      AsyncTaskCallBack<MySQLClientSession> callback,
      byte nextPacketId) {
    QueryUtil.COMMAND.requestEmptyPacket(mysql, nextPacketId, callback);
  }

  private static class LoadDataRequestTask implements ResultSetTask {

    String fileName;

    @Override
    public void onFinished(MySQLClientSession mysql, boolean success, String errorMessage) {
      AsyncTaskCallBack<MySQLClientSession> callBack = mysql.getCallBackAndReset();
      if (success) {
        callBack.finished(mysql, this, success, fileName, errorMessage);
      } else {
        callBack.finished(mysql, this, success, errorMessage, null);
      }

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
