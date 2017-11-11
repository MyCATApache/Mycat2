package io.mycat.mycat2.cmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.proxy.ProxyBuffer;

public class ComQuitCmd implements MySQLCommand{
	
	private static final Logger logger = LoggerFactory.getLogger(ComQuitCmd.class);

	public static final ComQuitCmd INSTANCE = new ComQuitCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		session.close(true, "client closed");
		return true;
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
		if(sessionCLosed){
			if(session.getProxyBuffer()!=null){
				session.bufPool.recycle(session.getProxyBuffer().getBuffer());
				session.setProxyBuffer(null);
			} 
			session.unbindAllBackend();
		}
	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub
		
	}

}
