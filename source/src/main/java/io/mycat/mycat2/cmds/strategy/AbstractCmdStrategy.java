package io.mycat.mycat2.cmds.strategy;

import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.CmdStrategy;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mysql.packet.MySQLPacket;

public abstract class AbstractCmdStrategy implements CmdStrategy {
	
	/**
	 * 进行MySQL命令的处理的容器
	 */
	protected Map<Byte, MySQLCommand> MYCOMMANDMAP = new HashMap<>();
	
	/**
	 * 进行SQL命令的处理的容器
	 */
	protected Map<Byte, MySQLCommand> MYSQLCOMMANDMAP = new HashMap<>();

	/**
	 * sqlparser
	 */
	protected BufferSQLParser parser = new BufferSQLParser();

	public AbstractCmdStrategy(){
		initMyCmdHandler();
		initMySqlCmdHandler();
	}
	
	protected abstract void initMyCmdHandler();
	
	protected abstract void initMySqlCmdHandler();
	
	@Override
	public MySQLCommand getMyCommand(MycatSession session) {
		MySQLCommand command = null;
		if(MySQLPacket.COM_QUERY==(byte)session.curMSQLPackgInf.pkgType){
			command = doGetMySQLCommand(session);
		}else{
			command = doGetMyCommand(session);
		}
		return command!=null?command:DirectPassthrouhCmd.INSTANCE;
	}
	
	/**
	 * 模板方法,默认的获取  my 命令处理器的方法，子类可以覆盖
	 * @param session
	 * @return
	 */
	protected MySQLCommand doGetMyCommand(MycatSession session){
		return MYCOMMANDMAP.get((byte)session.curMSQLPackgInf.pkgType);
	}
	
	/**
	 * 模板方法,默认的获取 sql 命令处理器的方法，子类可以覆盖
	 * @param session
	 * @return
	 */
	protected MySQLCommand doGetMySQLCommand(MycatSession session){
		parser.parse(session.proxyBuffer.getBuffer(), session.curMSQLPackgInf.startPos+MySQLPacket.packetHeaderSize+1,
				session.curMSQLPackgInf.pkgLength - MySQLPacket.packetHeaderSize - 1, session.sqlContext);
		System.out.println("getSQLType(0) : "+session.sqlContext.getSQLType(0)+" getSQLType() : "+session.sqlContext.getSQLType());
		return MYSQLCOMMANDMAP.get(session.sqlContext.getSQLType(0));
	}
}
