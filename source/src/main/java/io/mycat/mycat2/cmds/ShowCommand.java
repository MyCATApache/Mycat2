package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;

/**
 * 负责处理Show命令
 * @author wuzhihui
 *
 */
public class ShowCommand implements SQLCommand{

	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

}
