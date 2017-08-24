package io.mycat.mycat2.tasks;

import java.io.IOException;

import io.mycat.mycat2.AbstractMySQLSession;

/**
 * 异步任务回调接口
 * 
 * @author wuzhihui
 *
 */
public interface AsynTaskCallBack<T extends AbstractMySQLSession> {

	void finished(T session, Object sender, boolean success, Object result) throws IOException;
}
