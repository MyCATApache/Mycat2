package io.mycat.proxy.callback;

import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;

public interface ResultSetCallBack<T> extends TaskCallBack<ResultSetCallBack<T>> {

  void onFinishedSendException(Exception exception, Object sender, Object attr);

  void onFinishedException(Exception exception, Object sender, Object attr);

  void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender, Object attr);

  void onErrorPacket(ErrorPacket errorPacket,boolean monopolize, MySQLClientSession mysql, Object sender, Object attr);
  interface GetTextResultSetTransforCallBack extends
      ResultSetCallBack<GetTextResultSetTransforCallBack> {

    void onValue(int columnIndex, MySQLPacket packet, int type, int startIndex);
  }

  interface GetBinaryResultSetTransforCallBack extends
      ResultSetCallBack<GetBinaryResultSetTransforCallBack> {

    void onValue(int columnIndex, MySQLPacket packet, int type, int startIndex);

    void onNull(int columnIndex, int type, int startIndex);
  }
}