package io.mycat.mycat2.tasks;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;

/**
 * 异步任务回调接口
 * 
 * @author wuzhihui
 *
 */
public interface AsynTaskCallBack {

	void finished(MySQLSession session, Object sender, boolean success, Object result) throws IOException;
}
