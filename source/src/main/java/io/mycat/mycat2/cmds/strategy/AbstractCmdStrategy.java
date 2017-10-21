package io.mycat.mycat2.cmds.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.CmdStrategy;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.SQLAnnotationCmd;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;
import io.mycat.mycat2.sqlannotations.AnnotationProcessor;
import io.mycat.mycat2.sqlannotations.CacheResult;
import io.mycat.mycat2.sqlannotations.CacheResultMeta;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
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
	
	private Map<Byte,SQLAnnotation> staticAnnontationMap = new HashMap<>();

	/**
	 * sqlparser
	 */
	protected BufferSQLParser parser = new BufferSQLParser();

	public AbstractCmdStrategy(){
		initMyCmdHandler();
		initMySqlCmdHandler();
		initStaticAnnotation();
	}
	
	private void initStaticAnnotation(){
		CacheResultMeta cacheResultMeta = new CacheResultMeta();
		SQLAnnotation cacheResult = new CacheResult();
		cacheResult.setSqlAnnoMeta(cacheResultMeta);
		staticAnnontationMap.put(BufferSQLContext.ANNOTATION_SQL_CACHE,cacheResult);
	}
	
	protected abstract void initMyCmdHandler();
	
	protected abstract void initMySqlCmdHandler();
	
	@Override
	final public void matchMySqlCommand(MycatSession session) {
		
		MySQLCommand  command = null;
		if(MySQLPacket.COM_QUERY==(byte)session.curMSQLPackgInf.pkgType){
			/**
			 * sqlparser
			 */
			BufferSQLParser parser = new BufferSQLParser();
			int rowDataIndex = session.curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize +1 ;
			int length = session.curMSQLPackgInf.pkgLength -  MySQLPacket.packetHeaderSize - 1 ;
			parser.parse(session.proxyBuffer.getBuffer(), rowDataIndex, length, session.sqlContext);
			byte sqltype = session.sqlContext.getSQLType()!=0?session.sqlContext.getSQLType():session.sqlContext.getCurSQLType();
			System.out.println(session.sqlContext.getRealSQL(0));
			command = MYSQLCOMMANDMAP.get(sqltype);
		}else{
			command = MYCOMMANDMAP.get((byte)session.curMSQLPackgInf.pkgType);
		}
		if(command==null){
			command = DirectPassthrouhCmd.INSTANCE;
		}

		/**
		 * 设置原始处理命令
		 */
		session.curSQLCommand = command;
		processAnnotation(session, staticAnnontationMap);
	}
	
	/**
	 * 处理动态注解和静态注解
	 * 动态注解 会覆盖静态注解
	 * @param session
	 */
	public void processAnnotation(MycatSession session,Map<Byte,SQLAnnotation> staticAnnontationMap){
				
		BufferSQLContext context = session.sqlContext;
		
		SQLAnnotationChain chain = null;
		
		SQLAnnotation staticAnno = staticAnnontationMap.get(context.getAnnotationType());
		
		/**
		 * 处理静态注解
		 */
		if(staticAnno!=null){
			chain = new SQLAnnotationChain();
			SQLAnnotationCmd  annoCmd = staticAnno.getSqlAnnoMeta().getSQLAnnotationCmd();
			annoCmd.setSqlAnnotationChain(chain);
			chain.addCmdChain(staticAnno, annoCmd);
		}
		
		/**
		 * 处理动态注解
		 */
		List<SQLAnnotation> actions = new ArrayList<>(30);
		if(AnnotationProcessor.getInstance().parse(session.sqlContext, session, actions)){
			if(!actions.isEmpty()){
				chain = chain==null?new SQLAnnotationChain():chain;
				for(SQLAnnotation f:actions){
					if(!f.apply(session,chain)){
						break;
					}
				}
			}
		}
		
		if(chain!=null){
			chain.setTarget(session.curSQLCommand);
			chain.build();
			SQLAnnotationCmd annoCmd = new SQLAnnotationCmd();
			annoCmd.setSqlAnnotationChain(chain);
			session.curSQLCommand = annoCmd;
		}
	}
}
