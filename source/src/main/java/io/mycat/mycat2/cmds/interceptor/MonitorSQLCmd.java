package io.mycat.mycat2.cmds.interceptor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DefaultMySQLCommand;

public class MonitorSQLCmd extends DefaultMySQLCommand {
	
	public static final MonitorSQLCmd INSTANCE = new MonitorSQLCmd();
	
	private static final Logger logger = LoggerFactory.getLogger(MonitorSQLCmd.class);
	
	private MonitorSQLCmd(){}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		return super.procssSQL(session);
	}
	
	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		return super.onFrontWriteFinished(session);
	}
}
