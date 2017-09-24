package io.mycat.mycat2.advice.impl;

import java.io.IOException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DefaultInvocation;

public class SQLCach extends DefaultInvocation implements Function<MycatSession,Boolean> {
	
	public static final SQLCach INSTANCE = new SQLCach();
	
	private static final Logger logger = LoggerFactory.getLogger(SQLCach.class);
	
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
		logger.debug("========================> SQLCach {}",session.sqlContext.getRealSQL(0));
		return super.procssSQL(session);
	}
	
	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		logger.debug("========================> SQLCach onFrontWriteFinished ");
		return super.onFrontWriteFinished(session);
	}

}
