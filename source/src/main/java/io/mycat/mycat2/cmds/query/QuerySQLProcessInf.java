package io.mycat.mycat2.cmds.query;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.sqlparser.NewSQLContext;

/**
 * 进行查询SQL的处理
 * 
 * @since 2017年8月15日 下午6:23:25
 * @version 0.0.1
 * @author liujun
 */
public interface QuerySQLProcessInf {

	/**
	 * 进行查询的SQL处理
	 * 
	 * @param session
	 *            会话
	 * @param sqlContext
	 *            sql处理的上下文对象
	 * @throws IOException
	 *             异常
	 */
	public void querySqlProc(MySQLSession session, NewSQLContext sqlContext) throws IOException;

}
