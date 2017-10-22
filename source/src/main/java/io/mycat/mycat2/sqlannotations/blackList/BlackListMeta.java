package io.mycat.mycat2.sqlannotations.blackList;

import io.mycat.mycat2.cmds.SQLAnnotationCmd;
import io.mycat.mycat2.cmds.interceptor.BlockSqlCmd;
import io.mycat.mycat2.sqlannotations.SQLAnnotationMeta;

public class BlackListMeta implements SQLAnnotationMeta {
	
	private boolean allow;

	@Override
	public SQLAnnotationCmd getSQLAnnotationCmd() {
		return new BlockSqlCmd();
	}

	public boolean isAllow() {
		return allow;
	}

	public void setAllow(boolean allow) {
		this.allow = allow;
	}

}
