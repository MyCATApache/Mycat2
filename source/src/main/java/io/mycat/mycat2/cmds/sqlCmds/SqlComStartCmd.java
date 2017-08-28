package io.mycat.mycat2.cmds.sqlCmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mysql.AutoCommit;

public class SqlComStartCmd extends DirectPassthrouhCmd{
	
	private static final Logger logger = LoggerFactory.getLogger(SqlComStartCmd.class);

	public static final SqlComStartCmd INSTANCE = new SqlComStartCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		
		//TODO   start transaction;  start slave;  暂时还无法区分
		if(NewSQLContext.TRANSACTION_SQL==session.sqlContext.getSQLType()){
			
		}
		
		/*
		 * 开启事务,
		 * TODO 事务兼容性完善.
		 */
		session.autoCommit=AutoCommit.OFF;
		super.procssSQL(session);
		return false;
	}
	
}
