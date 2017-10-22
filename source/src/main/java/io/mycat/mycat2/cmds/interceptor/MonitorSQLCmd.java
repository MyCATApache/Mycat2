package io.mycat.mycat2.cmds.interceptor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.SQLAnnotationCmd;

public class MonitorSQLCmd extends SQLAnnotationCmd {
		
	private static final Logger logger = LoggerFactory.getLogger(MonitorSQLCmd.class);
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {

		logger.debug("=====>   MonitorSQLCmd   processSQL");
		return super.procssSQL(session);
	}
	
	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {

		logger.debug("=====>   MonitorSQLCmd   onFrontWriteFinished");
		return super.onFrontWriteFinished(session);
	}
}
