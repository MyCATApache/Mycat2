package io.mycat.mycat2.cmds.interceptor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DefaultMySQLCommand;

public class SQLCachCmd extends DefaultMySQLCommand {
	
	public static final SQLCachCmd INSTANCE = new SQLCachCmd();
	
	private static final Logger logger = LoggerFactory.getLogger(SQLCachCmd.class);
	
	private SQLCachCmd(){}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		return super.procssSQL(session);
	}
	
//	@Override
//	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
//		logger.debug("========================> SQLCachCmd onFrontWriteFinished ");
//		return super.onFrontWriteFinished(session);
//	}
}
