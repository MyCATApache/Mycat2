package io.mycat.mycat2.tasks;

import io.mycat.proxy.BackendIOHandler;
import io.mycat.proxy.UserProxySession;

/**
 * 子任务，在某些NIOProxyHandler中会使用，比如获取后端连接，同步后端连接
 * 
 * @author wuzhihui
 *
 */
public interface BackendIOTask<T extends UserProxySession > extends BackendIOHandler<T>{

	/**
	 * 任务完成后回调
	 * @param callback
	 */
	void setCallback(AsynTaskCallBack callBack);

}
