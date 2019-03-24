package io.mycat.mycat2.tasks;

import io.mycat.proxy.Session;

import java.io.IOException;

/**
 * 异步任务回调接口
 * 
 * @author wuzhihui
 *
 */
public interface AsynTaskCallBack<T extends Session> {

	void finished(T session, Object sender, boolean success, Object result) throws IOException;
}
