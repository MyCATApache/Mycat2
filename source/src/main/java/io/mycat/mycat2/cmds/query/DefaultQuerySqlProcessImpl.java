package io.mycat.mycat2.cmds.query;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.DefaultSqlCommand;
import io.mycat.mycat2.sqlparser.NewSQLContext;

/**
 * 默认的查询SQL处理
 * 
 * @since 2017年8月15日 下午6:29:45
 * @version 0.0.1
 * @author liujun
 */
public class DefaultQuerySqlProcessImpl implements QuerySQLProcessInf {

	/**
	 * 工厂方法实例
	 */
	public static final DefaultQuerySqlProcessImpl INSTANCE = new DefaultQuerySqlProcessImpl();

	@Override
	public void querySqlProc(MySQLSession session, NewSQLContext sqlContext) throws IOException {
		session.curSQLCommand = new DefaultSqlCommand();
		if (session.curSQLCommand.procssSQL(session, false)) {
			session.curSQLCommand.clearResouces(false);
		}
	}

}
