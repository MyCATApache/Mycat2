package io.mycat.proxy.handler;

import io.mycat.proxy.session.Session;
import java.nio.channels.SelectionKey;

/**
 * @author jamie12221
 *  date 2019-05-20 20:13
 **/
public interface BackendNIOHandler<T extends Session> extends NIOHandler<T> {

  /**
   * 只有mycat主动发起连接的handler才实现此方法
   */
  default void onConnect(SelectionKey curKey, T session, boolean success, Exception throwable) {
    assert false;
  }
}
