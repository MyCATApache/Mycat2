package io.mycat.mycat2.advice.impl;

import java.io.IOException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DefaultInvocation;

/**
 * SQL 监控需要做成异步
 * @author yanjunli
 *
 */
public class MonintorSQL extends DefaultInvocation implements Function<MycatSession,Boolean> {
	
	public static final MonintorSQL INSTANCE = new MonintorSQL();
	
	private static final Logger logger = LoggerFactory.getLogger(MonintorSQL.class);

	/**
	 * 组装 mysqlCommand
	 */
	@Override
	public Boolean apply(MycatSession session) {
		setCommand(session.curSQLCommand);
		session.curSQLCommand = this;
		return Boolean.TRUE;
	}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		logger.debug("========================> MonintorSQL {}",session.sqlContext.getRealSQL(0));
		return super.procssSQL(session);
	}
	
	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		logger.debug("========================> MonintorSQL onFrontWriteFinished ");
		return super.onFrontWriteFinished(session);
	}
}
