package io.mycat.mycat2.cmds.sqlCmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mysql.AutoCommit;

public class SqlComBeginCmd extends DirectPassthrouhCmd{
	
	private static final Logger logger = LoggerFactory.getLogger(SqlComBeginCmd.class);

	public static final SqlComBeginCmd INSTANCE = new SqlComBeginCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		/*
		 * 开启事务,
		 * TODO 事务兼容性完善.
		 */
		session.autoCommit=AutoCommit.OFF;
		super.procssSQL(session);
		return false;
	}
}
