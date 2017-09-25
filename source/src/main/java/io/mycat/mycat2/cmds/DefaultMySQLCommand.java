package io.mycat.mycat2.cmds;

import java.io.IOException;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;

/**
 * 默认的mysql命令实现
 * @author yanjunli
 *
 */
public class DefaultMySQLCommand implements MySQLCommand{

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		return session.getCmdChain().getNextSQLCommand().onBackendResponse(session);
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		return session.getCmdChain().getNextSQLCommand().onBackendClosed(session, normal);
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		return session.getCmdChain().getNextSQLCommand().onFrontWriteFinished(session);
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		return session.getCmdChain().getNextSQLCommand().onBackendWriteFinished(session);
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		session.getCmdChain().getNextSQLCommand().clearFrontResouces(session, sessionCLosed);
	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		session.getCmdChain().getNextSQLCommand().clearBackendResouces(session, sessionCLosed);
	}

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		return session.getCmdChain().getNextSQLCommand().procssSQL(session);
	}
}
