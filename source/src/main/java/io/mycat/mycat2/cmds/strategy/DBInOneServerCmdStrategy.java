package io.mycat.mycat2.cmds.strategy;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.ComChangeUserCmd;
import io.mycat.mycat2.cmds.ComInitDB;
import io.mycat.mycat2.cmds.ComPingCmd;
import io.mycat.mycat2.cmds.ComQuitCmd;
import io.mycat.mycat2.cmds.ComStatisticsCmd;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.NotSupportCmd;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

public class DBInOneServerCmdStrategy extends AbstractCmdStrategy{	
	
	public static final DBInOneServerCmdStrategy INSTANCE = new DBInOneServerCmdStrategy();

	@Override
	protected void initMyCmdHandler() {
		MYCOMMANDMAP.put(MySQLCommand.COM_QUIT,         			   ComQuitCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_INIT_DB,      			   ComInitDB.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_QUERY, DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_FIELD_LIST, DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_CREATE_DB,    			   NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_DROP_DB,      			   NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_REFRESH,      			   DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_SHUTDOWN,     			   NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_STATISTICS,   			   ComStatisticsCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_PROCESS_INFO, 			   DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_CONNECT,      			   NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_PROCESS_KILL, 			   DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_DEBUG,        			   NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_PING,         			   ComPingCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_TIME,         			   NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_DELAYED_INSERT,           NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_CHANGE_USER,              ComChangeUserCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_BINLOG_DUMP,              DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_TABLE_DUMP,               DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_CONNECT_OUT,              NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_REGISTER_SLAVE,           NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_STMT_PREPARE,             DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_STMT_EXECUTE,             DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_STMT_SEND_LONG_DATA,      DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_STMT_CLOSE,      		   DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_STMT_RESET,      		   DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_SET_OPTION,      		   DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_STMT_FETCH,      		   DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_DAEMON,      			   NotSupportCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_BINLOG_DUMP_GTID,         DirectPassthrouhCmd.INSTANCE);
		MYCOMMANDMAP.put(MySQLCommand.COM_RESET_CONNECTION,         DirectPassthrouhCmd.INSTANCE);
	}

	@Override
	protected void initMySqlCmdHandler() {
		MYSQLCOMMANDMAP.put(BufferSQLContext.INSERT_SQL, DirectPassthrouhCmd.INSTANCE);
		MYSQLCOMMANDMAP.put(BufferSQLContext.UPDATE_SQL, DirectPassthrouhCmd.INSTANCE);
        MYSQLCOMMANDMAP.put(BufferSQLContext.DROP_SQL, DirectPassthrouhCmd.INSTANCE);
		//MYSQLCOMMANDMAP.put(BufferSQLContext.COMMIT_SQL, SqlComCommitCmd.INSTANCE);
		//MYSQLCOMMANDMAP.put(BufferSQLContext.ROLLBACK_SQL, SqlComRollBackCmd.INSTANCE);
		MYSQLCOMMANDMAP.put(BufferSQLContext.SELECT_SQL, DirectPassthrouhCmd.INSTANCE);
		//MYSQLCOMMANDMAP.put(BufferSQLContext.BEGIN_SQL, SqlComBeginCmd.INSTANCE);
		//MYSQLCOMMANDMAP.put(BufferSQLContext.START_SQL, SqlComStartCmd.INSTANCE);
		//MYSQLCOMMANDMAP.put(BufferSQLContext.USE_SQL, SqlComStartCmd.INSTANCE);
		//MYSQLCOMMANDMAP.put(BufferSQLContext.SHUTDOWN_SQL, SqlComShutdownCmd.INSTANCE);
	}
}
