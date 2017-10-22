package io.mycat.mycat2.cmds.interceptor;

import java.io.IOException;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;

/**
 * 抽象动态注解 命令实现，动态注解命令需要继承该方法
 * @author yanjunli
 *
 */
public class SQLAnnotationCmd implements MySQLCommand {
		
	private SQLAnnotationChain sqlAnnoChain;
	
	private String errMsg;
	
	public void setSqlAnnotationChain(SQLAnnotationChain sqlAnnoChain){
		this.sqlAnnoChain = sqlAnnoChain;
	}
	
	protected SQLAnnotationChain getSQLAnnotationChain(){
		return sqlAnnoChain;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		return sqlAnnoChain.next().onBackendResponse(session);
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		return sqlAnnoChain.next().onBackendClosed(session, normal);
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		return sqlAnnoChain.next().onFrontWriteFinished(session);
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		return sqlAnnoChain.next().onBackendWriteFinished(session);
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		sqlAnnoChain.next().clearFrontResouces(session, sessionCLosed);
	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		sqlAnnoChain.next().clearBackendResouces(session, sessionCLosed);
	}

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {		
		return sqlAnnoChain.next().procssSQL(session);
	}

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		this.errMsg = errMsg;
	}

}
