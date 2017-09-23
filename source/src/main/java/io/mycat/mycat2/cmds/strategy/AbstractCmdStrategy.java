package io.mycat.mycat2.cmds.strategy;

import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.CmdStrategy;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
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
		
		/**
		 * sqlparser
		 */
//		BufferSQLParser parser = new BufferSQLParser();
//		int rowDataIndex = session.curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize +1 ;
//		int length = session.curMSQLPackgInf.pkgLength -  MySQLPacket.packetHeaderSize - 1 ;
//		parser.parse(session.proxyBuffer.getBytes(rowDataIndex, length),session.sqlContext);

		return MYSQLCOMMANDMAP.get(session.sqlContext.getSQLType());
	}
}
