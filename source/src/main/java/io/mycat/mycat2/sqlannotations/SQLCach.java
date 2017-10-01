package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLCachCmd;

public class SQLCach extends SQLAnnotation {

	public static final SQLCach INSTANCE = new SQLCach();
	
	private static final Logger logger = LoggerFactory.getLogger(SQLCach.class);
	
	private static final SQLCachCmd cmd = SQLCachCmd.INSTANCE;
	
	/**
	 * 组装 mysqlCommand
	 */
	@Override
	public Boolean apply(MycatSession session) {
		session.getCmdChain().addCmdChain(this);
		return Boolean.TRUE;
	}

	@Override
	public void init(Object args) {

	}



	@Override
	public MySQLCommand getMySQLCommand() {
		return cmd;
	}

}
