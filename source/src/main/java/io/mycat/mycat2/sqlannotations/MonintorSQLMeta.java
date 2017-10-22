package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.cmds.interceptor.MonitorSQLCmd;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationCmd;

public class MonintorSQLMeta implements SQLAnnotationMeta {

	@Override
	public SQLAnnotationCmd getSQLAnnotationCmd() {
		return new MonitorSQLCmd();
	}

}
