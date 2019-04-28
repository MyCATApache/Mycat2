package io.mycat.proxy.task;

import io.mycat.proxy.session.Session;

public interface AsynTaskCallBack<T extends Session> {

	void finished(T session, Object sender, boolean success, Object result, Object attr) ;
}
