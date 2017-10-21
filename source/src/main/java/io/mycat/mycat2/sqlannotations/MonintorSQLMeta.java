package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.cmds.SQLAnnotationCmd;
import io.mycat.mycat2.cmds.interceptor.MonitorSQLCmd;

public class MonintorSQLMeta implements SQLAnnotationMeta {

	@Override
	public SQLAnnotationCmd getSQLAnnotationCmd() {
		return new MonitorSQLCmd();
	}

}
