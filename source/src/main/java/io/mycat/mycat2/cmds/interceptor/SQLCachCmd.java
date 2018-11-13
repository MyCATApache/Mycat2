package io.mycat.mycat2.cmds.interceptor;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.front.CacheExistsCheck;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SQLCachCmd extends SQLAnnotationCmd {

	private static final Logger logger = LoggerFactory.getLogger(SQLCachCmd.class);

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		
		logger.debug("=====>   SQLCacheCmd   processSQL");
		
		BufferSQLContext context = session.sqlContext;
		
		if(BufferSQLContext.SELECT_SQL != context.getSQLType()){
			String errmsg = " sqlType is invalid . sqlcache  type must be select !";
			session.sendErrorMsg(ErrorCode.ER_INVALID_DEFAULT,errmsg);
			logger.error(errmsg);
			return true;
		}
		
		if(BufferSQLContext.ANNOTATION_SQL_CACHE != context.getAnnotationType()){
			
			String errmsg = " annotationType is invalid . annotationType must be ANNOTATION_SQL_CACHE !";
			session.sendErrorMsg(ErrorCode.ER_INVALID_DEFAULT,errmsg);
			logger.error(errmsg);
			return true;
		}

		// 检查当前是否为缓存的SQL
		long cacheTime = context.getAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME);
		long cacheTimeOut = context.getAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT);
		logger.debug(" current sql  cacheTime is {},cacheTimeOut is {}",cacheTime,cacheTimeOut);

		// 放入缓存过期时间
        session.getAttrMap().put(SessionKey.CACHE_TIMEOUT, cacheTime);
		// 放入临近的过期时间
        session.getAttrMap().put(SessionKey.CACHE_TIMEOUT_CRITICAL, cacheTimeOut);
		// 放入sql语句
        session.getAttrMap().put(SessionKey.CACHE_SQL_STR,
				session.sqlContext.getRealSQL(0));

		if (null != session.curBackend) {
			// 设置后端为使用中
            session.curBackend.setIdle(false);
		}

        SeqContextList seqcontext = (SeqContextList) session.getAttrMap()
                .get(SessionKey.CACHE_MYCAT_CHAIN_SEQ);

		if (null == seqcontext) {
			seqcontext = new SeqContextList();
            session.getAttrMap().put(SessionKey.CACHE_MYCAT_CHAIN_SEQ, seqcontext);
		}

		seqcontext.clear();

		seqcontext.setSession(session);
		// // 首先添加缓存转换为sql
		// seqcontext.addExec(CacheReadBufferToSql.INSTANCE);
		// 然后为缓存是否存
		seqcontext.addExec(CacheExistsCheck.INSTANCE);

		try {
			seqcontext.nextExec();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			seqcontext.clear();
		}

		return super.procssSQL(session);
	}
	 @Override
	 public boolean onFrontWriteFinished(MycatSession session) throws
	 IOException {
	 logger.debug("========================> SQLCachCmd onFrontWriteFinished");
	 return super.onFrontWriteFinished(session);
	 }
}
