package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.proxy.BackendIOHandler;

/**
 * 子任务，在某些NIOProxyHandler中会使用，比如获取后端连接，同步后端连接
 * 
 * @author wuzhihui
 *
 */
public interface BackendIOTask extends BackendIOHandler<MySQLSession>{

	/**
	 * 任务完成后回调
	 * @param callback
	 */
	void setCallback(AsynTaskCallBack callBack);

}
