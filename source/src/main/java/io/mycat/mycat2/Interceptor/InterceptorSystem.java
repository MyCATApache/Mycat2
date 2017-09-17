package io.mycat.mycat2.Interceptor;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MyCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
/*
 * */
public  class InterceptorSystem {
	
	private static Logger logger = LoggerFactory.getLogger(InterceptorSystem.class);
	public static final InterceptorSystem INSTANCE = new InterceptorSystem();
	public boolean onFrontReadIntercept(MycatSession session) throws IOException {
		boolean interceptorResult = true;
		boolean prcessSQLResult = true;
		final List<Interceptor> interceptorList = MycatSession.interceptorList;
		for(int i = 0; interceptorResult  && i < interceptorList.size(); i++) {
			interceptorResult = interceptorList.get(i).intercept(session);
			MyCommand sqlCmd = (MyCommand) session.getSQLCmd(interceptorList.get(i));
			if(sqlCmd != null) {
				boolean tmpResult = sqlCmd.procssSQL(session);
				if(!tmpResult){
					prcessSQLResult = false;
					logger.debug("{} prcess {} not finish", sqlCmd, session);;
					//sqlCmd.clearFrontResouces(session, false);
				}
				//已经有cmd进行应答了 所有的cmd中只有一个应答前段 请求后端的
				if(session.curSQLCommand != null) {
					interceptorResult = false;
				}
			}
		}
		
		if(prcessSQLResult) {
			session.clearSQLCmdsFrontResouces(false);
		}
		return true;
	}

	public  boolean onBackendReadIntercept(MySQLSession session) throws IOException {
		boolean responseResult = true;
		MycatSession mycatSession = session.getMycatSession();
		final List<Interceptor> interceptorList = MycatSession.interceptorList;
		for(int i = interceptorList.size() -1; i >= 0; i--) {
			Interceptor inteceptor = interceptorList.get(i);
			MyCommand sqlCmd = mycatSession.getSQLCmd(inteceptor );
			if(sqlCmd != null) {
				boolean tmpResponseResult = sqlCmd.onBackendResponse(session);
				if(!tmpResponseResult) {
					logger.debug("{} onBackendResponse {} not finish", sqlCmd, session);;
					responseResult = false;
				}
			}
		}
		if(responseResult) {
			mycatSession.clearSQLCmdsBackendResouces(session, false);
		}
		return false;
	}
	
	public boolean clearFrontWriteFinishedIntercept(MycatSession session) throws IOException{
		if(session.curSQLCommand.onFrontWriteFinished(session)) {
			session.clearSQLCmdsFrontResouces(false);
		}
		return true;
	}
	public boolean clearBackendResoucesIntercept(MySQLSession mysqlSession) throws IOException{
//		boolean interceptorResult = true;
//		boolean result = true;
//		MycatSession mycatSession = mysqlSession.getMycatSession();
//		final List<Interceptor> interceptorList = MycatSession.interceptorList;
//		for(int i = 0; interceptorResult  && i < interceptorList.size(); i++) {
//			MyCommand sqlCmd = mycatSession.getSQLCmd(interceptorList.get(i));
//			if(sqlCmd != null) {
//				boolean tmpResult = sqlCmd.onBackendWriteFinished(mysqlSession);
//				if(!tmpResult){
//					result = false;
//					logger.debug("{} prcess back {} not write finish", sqlCmd, mysqlSession);;
//					//sqlCmd.clearFrontResouces(session, false);
//				}
//			}
//		}
		MycatSession mycatSession = mysqlSession.getMycatSession();
		if(mycatSession.curSQLCommand.onBackendWriteFinished(mysqlSession)) {
			mycatSession.clearSQLCmdsBackendResouces(mysqlSession, false);
		}
		return true;
	}
	
	public boolean onBackendClosedIntercept(MySQLSession mysqlSession, boolean normal) throws IOException{
		MycatSession mycatSession = mysqlSession.getMycatSession();
		final List<Interceptor> interceptorList = MycatSession.interceptorList;
		for(int i = 0;  i < interceptorList.size(); i++) {
			MyCommand sqlCmd = mycatSession.getSQLCmd(interceptorList.get(i));
			sqlCmd.onBackendClosed(mysqlSession, normal);
		}
		return true;
	}
}
