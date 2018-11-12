package io.mycat.mycat2;

import java.io.IOException;

import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.util.ParseUtil;

/**
 * 负责处理SQL命令
 * 
 * @author wuzhihui
 *
 * @param <T>
 */
public interface MySQLCommand  {
	/**
	 * 收到后端应答
	 * 
	 * @param session
	 *            后端MySQLSession
	 * @return
	 * @throws IOException
	 */
	public boolean onBackendResponse(MySQLSession session) throws IOException;

	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException;

	public boolean onFrontWriteFinished(MycatSession session) throws IOException;

	public boolean onBackendWriteFinished(MySQLSession session) throws IOException;

	/**
	 * 清理资源，只清理自己产生的资源（如创建了Buffer，以及Session中放入了某些对象）
	 * 
	 * @param socketClosed
	 *            是否因为Session关闭而清理资源，此时应该彻底清理
	 */
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed);

	/**
	 * 清理资源，只清理自己产生的资源（如创建了Buffer，以及Session中放入了某些对象）
	 * 
	 * @param socketClosed
	 *            是否因为Session关闭而清理资源，此时应该彻底清理
	 */
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed);

	/**
	 * 直接应答请求报文，如果是直接应答的，则此方法调用一次就完成了，如果是靠后端响应后才应答，则至少会调用两次，
	 * 
	 * @param session
	 * @return 是否完成了应答
	 */
	public boolean procssSQL(MycatSession session) throws IOException;

	/**
	 * 当前为load data的响应包
	 */
	public static final byte LOAD_DATA_PACKET = (byte) 0xfb;

	// 前端报文类型
	/**
	 * none, this is an internal thread state
	 */
	public static final byte COM_SLEEP = 0;

	/**
	 * mysql_close
	 */
	public static final byte COM_QUIT = 1;

	public static final int COM_QUIT_PACKET_LENGTH = 1;

	/**
	 * mysql_select_db
	 */
	public static final byte COM_INIT_DB = 2;

	/**
	 * mysql_real_query
	 */
	public static final byte COM_QUERY = 3;

	/**
	 * mysql_list_fields
	 */
	public static final byte COM_FIELD_LIST = 4;

	/**
	 * mysql_create_db (deprecated)
	 */
	public static final byte COM_CREATE_DB = 5;

	/**
	 * mysql_drop_db (deprecated)
	 */
	public static final byte COM_DROP_DB = 6;

	/**
	 * mysql_refresh
	 */
	public static final byte COM_REFRESH = 7;

	/**
	 * mysql_shutdown
	 */
	public static final byte COM_SHUTDOWN = 8;

	/**
	 * mysql_stat
	 */
	public static final byte COM_STATISTICS = 9;

	/**
	 * mysql_list_processes
	 */
	public static final byte COM_PROCESS_INFO = 10;

	/**
	 * none, this is an internal thread state
	 */
	public static final byte COM_CONNECT = 11;

	/**
	 * mysql_kill
	 */
	public static final byte COM_PROCESS_KILL = 12;

	/**
	 * mysql_dump_debug_info
	 */
	public static final byte COM_DEBUG = 13;

	/**
	 * mysql_ping
	 */
	public static final byte COM_PING = 14;

	/**
	 * none, this is an internal thread state
	 */
	public static final byte COM_TIME = 15;

	/**
	 * none, this is an internal thread state
	 */
	public static final byte COM_DELAYED_INSERT = 16;

	/**
	 * mysql_change_user
	 */
	public static final byte COM_CHANGE_USER = 17;

	/**
	 * used by slave server mysqlbinlog
	 */
	public static final byte COM_BINLOG_DUMP = 18;

	/**
	 * used by slave server to get master table
	 */
	public static final byte COM_TABLE_DUMP = 19;

	/**
	 * used by slave to log connection to master
	 */
	public static final byte COM_CONNECT_OUT = 20;

	/**
	 * used by slave to register to master
	 */
	public static final byte COM_REGISTER_SLAVE = 21;

	/**
	 * mysql_stmt_prepare
	 */
	public static final byte COM_STMT_PREPARE = 22;

	/**
	 * mysql_stmt_execute
	 */
	public static final byte COM_STMT_EXECUTE = 23;

	/**
	 * mysql_stmt_send_long_data
	 */
	public static final byte COM_STMT_SEND_LONG_DATA = 24;

	/**
	 * mysql_stmt_close
	 */
	public static final byte COM_STMT_CLOSE = 25;

	/**
	 * mysql_stmt_reset
	 */
	public static final byte COM_STMT_RESET = 26;

	/**
	 * mysql_set_server_option
	 */
	public static final byte COM_SET_OPTION = 27;

	/**
	 * mysql_stmt_fetch
	 */
	public static final byte COM_STMT_FETCH = 28;

	/**
	 * mysql_stmt_fetch
	 */
	public static final byte COM_DAEMON = 29;

	/**
	 * mysql_stmt_fetch
	 */
	public static final byte COM_BINLOG_DUMP_GTID = 30;

	/**
	 * mysql_stmt_fetch
	 */
	public static final byte COM_RESET_CONNECTION = 31;

}
