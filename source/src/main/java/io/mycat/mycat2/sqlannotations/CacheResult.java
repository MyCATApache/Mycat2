package io.mycat.mycat2.sqlannotations;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLCachCmd;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

/**
 * Created by jamie on 2017/9/15.
 */
public class CacheResult extends SQLAnnotation {

	private static final Logger logger = LoggerFactory.getLogger(CacheResult.class);
	
	/*
	 * 如果动态注解和静态注解，同时用到 。  需要 在  getMySQLCommand 返回当前 sqlcommand
	 */
	final static private MySQLCommand command = SQLCachCmd.INSTANCE;	
	
	/**
	 * 缓存时间
	 */
	private long cacheTime = 0;
		
	/**
	 * 访问次数
	 */
	private long accessCount = 0;
	
	
	/**
	 * 动态注解 组装 mysqlCommand chain
	 */
	@Override
	public Boolean apply(MycatSession session) {
		session.getCmdChain().addCmdChain(this);
		BufferSQLContext  context = session.sqlContext;
		context.setAnnotationType(BufferSQLContext.ANNOTATION_SQL_CACHE);
		context.setAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME,cacheTime);
		context.setAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT,accessCount);		
		return Boolean.TRUE;
	}

	@Override
	public void init(Object args) {
		Map argMap = (Map)args;
		cacheTime = (int)argMap.get("cache_time");
		accessCount = (int)argMap.get("access_count");
	}
	
	public CacheResult setCacheTime(long cacheTime){
		this.cacheTime = cacheTime;
		return this;
	}
	
	public CacheResult setAccessCount(long accessCount){
		this.accessCount = accessCount;
		return this;
	}



	@Override
	public MySQLCommand getMySQLCommand() {
		return command;
	}
}
