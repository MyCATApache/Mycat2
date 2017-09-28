package io.mycat.mycat2.cmds.interceptor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DefaultMySQLCommand;

public class SQLCachCmd extends DefaultMySQLCommand {
	
	public static final SQLCachCmd INSTANCE = new SQLCachCmd();
	
	private static final Logger logger = LoggerFactory.getLogger(SQLCachCmd.class);
	
	private SQLCachCmd(){}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		logger.debug("========================> SQLCachCmd {}", session.sqlContext.getRealSQL(0));

		BufferSQLContext context = session.sqlContext;

		// 检查当前是否为缓存的SQL
		if (BufferSQLContext.SELECT_SQL == context.getSQLType()
				&& BufferSQLContext.ANNOTATION_SQL_CACHE == context.getAnnotationType()) {
			long cacheTime = context.getAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME);
			long cacheTimeOut = context.getAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT);

			// 放入缓存过期时间
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_TIMEOUT.getKey(), cacheTime);
			// 放入临近的过期时间
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_TIMEOUT_CRITICAL.getKey(), cacheTimeOut);
			// 放入sql语句
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_SQL_STR.getKey(),
					session.sqlContext.getRealSQL(0));

			if (null != session.curBackend) {
				// 设置后端为使用中
				session.curBackend.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
			}

			SeqContextList seqcontext = (SeqContextList) session.getSessionAttrMap()
					.get(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey());

			if (null == seqcontext) {
				seqcontext = new SeqContextList();
				session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey(), seqcontext);
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
		}

		return super.procssSQL(session);
	}
	
//	@Override
//	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
//		logger.debug("========================> SQLCachCmd onFrontWriteFinished ");
//		return super.onFrontWriteFinished(session);
//	}
}
