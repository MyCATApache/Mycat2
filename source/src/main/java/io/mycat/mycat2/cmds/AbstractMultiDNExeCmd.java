package io.mycat.mycat2.cmds;

import java.io.IOException;


import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.MySQLCommand;
/**
 * 多节点执行SQL的抽象MySQLCommand类。
 * 需要修改SQLCommand接口，
 *  改为 public boolean procssSQL(MyCatSession session）
 *       public boolean onResponse(MySQLSession session)
 * @author wuzhihui
 *
 */
public class AbstractMultiDNExeCmd implements MySQLCommand{

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

}
