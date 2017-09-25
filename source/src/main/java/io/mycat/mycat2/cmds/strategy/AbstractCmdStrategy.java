package io.mycat.mycat2.cmds.strategy;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.advice.impl.intercept.SelelctAllow;
import io.mycat.mycat2.cmds.CmdStrategy;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;

import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mysql.packet.MySQLPacket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
	public void matchMySqlCommand(MycatSession session) {
		if(MySQLPacket.COM_QUERY==(byte)session.curMSQLPackgInf.pkgType){
			preMySQLCommand(session);
		}else{
			preMyCommand(session);
		}
	}
	
	/**
	 * 模板方法,默认的获取  my 命令处理器的方法，子类可以覆盖
	 * @param session
	 * @return
	 */
	protected void preMyCommand(MycatSession session){
		MySQLCommand  command = MYCOMMANDMAP.get((byte)session.curMSQLPackgInf.pkgType);
		if(command==null){
			command = DirectPassthrouhCmd.INSTANCE;
		}
		session.curSQLCommand.setCommand(command);
	}
	
	/**
	 * 模板方法,默认的获取 sql 命令处理器的方法，子类可以覆盖
	 * @param session
	 * @return
	 */
	protected void preMySQLCommand(MycatSession session){
		
		int rowDataIndex = session.curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize +1 ;
		int length = session.curMSQLPackgInf.pkgLength -  MySQLPacket.packetHeaderSize - 1 ;
		parser.parse(session.proxyBuffer.getBuffer(), rowDataIndex, length, session.sqlContext);
		
		MySQLCommand  command = MYSQLCOMMANDMAP.get(session.sqlContext.getSQLType());
		if(command==null){
			command = DirectPassthrouhCmd.INSTANCE;
		}
		session.curSQLCommand.setCommand(command);
		List<Function<MycatSession, Boolean>> actions = new ArrayList<>();
		if(AnnotationProcessor.getInstance().parse(session.sqlContext, session, actions)){
			//测试 模拟 动态注解获取到的 actions
//		actions.add(MonintorSQL.INSTANCE);
//		actions.add(SQLCach.INSTANCE);
//		actions.add(SelelctAllow.INSTANCE);
			//模拟命令组装过程
			System.out.println(actions.size());
			actions.forEach((i)->{
			SQLAnnotation annotation=	(SQLAnnotation)i;
				System.out.println(annotation.toString());

			});
			actions.stream().forEach(f->{f.apply(session);});
		}
	}
}
