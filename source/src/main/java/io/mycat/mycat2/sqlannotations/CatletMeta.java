package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.cmds.interceptor.CatletCmd;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationCmd;

public class CatletMeta implements SQLAnnotationMeta {
	
	/**
	 * 执行的hbt的类名称
	 * */
	private long clazz ;
	

	public long getClazz() {
		return clazz;
	}


	public void setClazz(long clazz) {
		this.clazz = clazz;
	}


	@Override
	public SQLAnnotationCmd getSQLAnnotationCmd() {
		return new CatletCmd();
	}
}
