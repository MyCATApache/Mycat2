package io.mycat.mycat2.tasks;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.proxy.NIOHandler;

/**
 * 子任务，在某些NIOProxyHandler中会使用，比如获取后端连接，同步后端连接
 * 
 * @author wuzhihui
 *
 */
public interface BackendIOTask<T extends AbstractMySQLSession> extends NIOHandler<T>{

	/**
	 * 任务完成后回调
	 * @param callback
	 */
	void setCallback(AsynTaskCallBack<T> callBack);

}
