package io.mycat.mycat2.Interceptor;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
/**
 * session中有一系列的处理的intercepter 用来设置某个sqlcmd是否需要进行处理
 * 如果需要应答需要设置MycatSession.curSQLCommand 表示为应答前段或者请求后端
 *    设置完intercepter,此时 之后的incepter就不会在进行处理
 * 
 * @author zwy
 *
 */
public  class InterceptorSystem {
	
	private static Logger logger = LoggerFactory.getLogger(InterceptorSystem.class);
	public static final InterceptorSystem INSTANCE = new InterceptorSystem();
	public boolean onFrontReadIntercept(MycatSession session) throws IOException {
		boolean interceptorResult = true;
		boolean prcessSQLResult = true;
		final List<Interceptor> interceptorList = MycatSession.interceptorList;
		for(int i = 0; interceptorResult  && i < interceptorList.size(); i++) {
			interceptorResult = interceptorList.get(i).intercept(session);
			MySQLCommand sqlCmd = session.getSQLCmd(interceptorList.get(i));
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
/*
 * */
	public  boolean onBackendReadIntercept(MySQLSession session) throws IOException {
		boolean responseResult = true;
		MycatSession mycatSession = session.getMycatSession();
		final List<Interceptor> interceptorList = MycatSession.interceptorList;
		for(int i = interceptorList.size() -1; i >= 0; i--) {
			Interceptor inteceptor = interceptorList.get(i);
			MySQLCommand sqlCmd = mycatSession.getSQLCmd(inteceptor );
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
			MySQLCommand sqlCmd = mycatSession.getSQLCmd(interceptorList.get(i));
			sqlCmd.onBackendClosed(mysqlSession, normal);
		}
		return true;
	}
}
