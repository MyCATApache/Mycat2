package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationCmd;


/**
 * SQL 监控需要做成异步
 * @author yanjunli
 *
 */
public class MonintorSQL extends SQLAnnotation {
	
	public static final MonintorSQL INSTANCE = new MonintorSQL();
	
	private static final Logger logger = LoggerFactory.getLogger(MonintorSQL.class);
	
	/**
	 * 组装 mysqlCommand
	 */
	@Override
	public boolean apply(MycatSession session,SQLAnnotationChain chain) {
		MonintorSQLMeta meta = (MonintorSQLMeta) getSqlAnnoMeta();
		SQLAnnotationCmd cmd = meta.getSQLAnnotationCmd();
		cmd.setSqlAnnotationChain(chain);
		chain.addCmdChain(this,cmd);
		return true;
	}

	@Override
	public void init(Object args) {
		MonintorSQLMeta meta = new MonintorSQLMeta();
		setSqlAnnoMeta(meta);
	}
}
