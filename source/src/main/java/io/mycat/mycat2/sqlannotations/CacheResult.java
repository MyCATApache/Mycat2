package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by jamie on 2017/9/15.
 */
public class CacheResult extends SQLAnnotation {

	private static final Logger logger = LoggerFactory.getLogger(CacheResult.class);
	
	/**
	 * 动态注解 组装 mysqlCommand chain
	 */
	@Override
	public boolean apply(MycatSession session,SQLAnnotationChain chain) {
		CacheResultMeta meta = (CacheResultMeta) getSqlAnnoMeta();
		//结果集缓存正在重构,当前版本存在bug,暂时去掉
//		SQLAnnotationCmd cmd = meta.getSQLAnnotationCmd();
//		cmd.setSqlAnnotationChain(chain);
//		chain.addCmdChain(this,cmd);

        BufferSQLContext context = session.sqlContext;
		context.setAnnotationType(BufferSQLContext.ANNOTATION_SQL_CACHE);
		context.setAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME,meta.getCacheTime());
		context.setAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT,meta.getAccessCount());		
		return true;
	}

	@Override
	public void init(Object args) {
		Map argMap = (Map)args;
		CacheResultMeta meta = new CacheResultMeta();
		meta.setAccessCount((int)argMap.get("access_count"));
		meta.setCacheTime((int)argMap.get("cache_time"));
		setSqlAnnoMeta(meta);
	}
}
