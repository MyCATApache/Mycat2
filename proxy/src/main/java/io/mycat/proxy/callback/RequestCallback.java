package io.mycat.proxy.callback;

import io.mycat.proxy.session.MySQLClientSession;

/**
 * @author jamie12221
 *  date 2019-05-22 11:18
 **/
public interface RequestCallback extends TaskCallBack<RequestCallback> {

  void onFinishedSend(MySQLClientSession session,
      Object sender,
      Object attr);

  void onFinishedSendException(Exception e,
      Object sender,
      Object attr);

}
