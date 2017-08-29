package io.mycat.mycat2.cmds.strategy;

import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.MyCommand;
import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.CmdStrategy;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mycat2.sqlparser.NewSQLParser;
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
	public MyCommand getMyCommand(MycatSession session) {
		MyCommand command = null;
		if(MySQLPacket.COM_QUERY==session.curMSQLPackgInf.pkgType){
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
	protected MyCommand doGetMyCommand(MycatSession session){
		return MYCOMMANDMAP.get(session.curMSQLPackgInf.pkgType);
	}
	
	/**
	 * 模板方法,默认的获取 sql 命令处理器的方法，子类可以覆盖
	 * @param session
	 * @return
	 */
	protected MyCommand doGetMySQLCommand(MycatSession session){
		NewSQLParser parser = new NewSQLParser();
		parser.parse(session.proxyBuffer.getBytes(session.curMSQLPackgInf.startPos+MySQLPacket.packetHeaderSize+1,
				session.curMSQLPackgInf.pkgLength - MySQLPacket.packetHeaderSize - 1), session.sqlContext);
		return MYSQLCOMMANDMAP.get(session.sqlContext.getSQLType());
	}
}
