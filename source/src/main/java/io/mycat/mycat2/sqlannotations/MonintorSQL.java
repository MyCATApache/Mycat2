package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.BlockSqlCmd;
import io.mycat.mycat2.cmds.interceptor.MonitorSQLCmd;

/**
 * SQL 监控需要做成异步
 * @author yanjunli
 *
 */
public class MonintorSQL implements SQLAnnotation {
	
	public static final MonintorSQL INSTANCE = new MonintorSQL();
	
	private static final Logger logger = LoggerFactory.getLogger(MonintorSQL.class);
	
	final private MySQLCommand command = MonitorSQLCmd.INSTANCE;

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
	public String getMethod() {
		return null;
	}

	@Override
	public void setMethod(String method) {

	}

	@Override
	public MySQLCommand getMySQLCommand() {
		return command;
	}

}
