package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;

public class SQLCach extends SQLAnnotation {

	public static final SQLCach INSTANCE = new SQLCach();
	
	private static final Logger logger = LoggerFactory.getLogger(SQLCach.class);
		
	/**
	 * 组装 mysqlCommand
	 */
	@Override
	public boolean apply(MycatSession context,SQLAnnotationChain chain) {
		return true;
	}

	@Override
	public void init(Object args) {

	}
}
