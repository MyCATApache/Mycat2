package io.mycat.mycat2.cmds;

import java.io.IOException;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.advice.Invocation;

/**
 * 默认的mysql命令实现
 * @author yanjunli
 *
 */
public class DefaultInvocation implements Invocation{
	
	protected MySQLCommand command;
		
	public DefaultInvocation(){}
	
	@Override
	public void setCommand(MySQLCommand command) {
		this.command = command;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		return command.onBackendResponse(session);
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		return command.onBackendClosed(session, normal);
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		return command.onFrontWriteFinished(session);
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		return command.onBackendWriteFinished(session);
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		command.clearFrontResouces(session, sessionCLosed);
	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		command.clearBackendResouces(session, sessionCLosed);
	}

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		return command.procssSQL(session);
	}
}
