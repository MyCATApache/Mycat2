package io.mycat.mycat2.sqlannotations;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationCmd;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

/**
 * Created by jamie on 2017/9/15.
 */
public class CatletResult extends SQLAnnotation {

	private static final Logger logger = LoggerFactory.getLogger(CatletResult.class);
	
	/**
	 * 动态注解 组装 mysqlCommand chain
	 */
	@Override
	public boolean apply(MycatSession session,SQLAnnotationChain chain) {
		CatletMeta meta = (CatletMeta) getSqlAnnoMeta();
		
		SQLAnnotationCmd cmd = meta.getSQLAnnotationCmd();
		cmd.setSqlAnnotationChain(chain);
		chain.addCmdChain(this,cmd);
		
		BufferSQLContext context = session.sqlContext;
		context.setAnnotationType(BufferSQLContext.ANNOTATION_CATLET);
		context.setAnnotationStringValue(BufferSQLContext.ANNOTATION_CATLET, meta.getClazz());
//		context.getAnnotationValue(meta.getClazz());
		return true;
	}

	@Override
	public void init(Object args) {
		Map argMap = (Map)args;
		CatletMeta meta = new CatletMeta();
		String clazz = (String) argMap.get("clazz");
		meta.setClazz(clazz);
		setSqlAnnoMeta(meta);
	}
}
