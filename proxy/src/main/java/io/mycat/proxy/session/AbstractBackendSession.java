package io.mycat.proxy.session;

import io.mycat.proxy.callback.TaskCallBack;
import io.mycat.proxy.handler.NIOHandler;

/**
 * @author jamie12221
 * @date 2019-05-22 02:18
 **/
public abstract class AbstractBackendSession<T extends AbstractSession> extends AbstractSession<T> {

  protected TaskCallBack callBack;

  public AbstractBackendSession(NIOHandler nioHandler,
      SessionManager<T> sessionManager) {
    super(nioHandler, sessionManager);
  }

  public <T> T getCallBack() {
    T callBack = (T) this.callBack;
    return callBack;
  }

  /**
   * 设置callback,一般是Task类设置
   */
  public void setCallBack(TaskCallBack callBack) {
    this.callBack = callBack;
  }

}
