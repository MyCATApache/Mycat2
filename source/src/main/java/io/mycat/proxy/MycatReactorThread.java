package io.mycat.proxy;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Stream;

import io.mycat.mycat2.beans.conf.SchemaBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mycat2.tasks.BackendSynchemaTask;
import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.util.ErrorCode;

/**
 *  mycat 多个Session会话
 * @author yanjunli
 *
 */
public class MycatReactorThread extends ProxyReactorThread<MycatSession> {
	
	protected final static Logger logger = LoggerFactory.getLogger(MycatReactorThread.class);
	
	// 存放后端连接的map
	protected Map<MySQLMetaBean, LinkedList<MySQLSession>> mySQLSessionMap = new HashMap<>();

	public MycatReactorThread(BufferPool bufPool) throws IOException {
		super(bufPool);
	}
	
	public void clearMySQLMetaBeanSession(MySQLMetaBean mySQLMetaBean,String reason){
		LinkedList<MycatSession> sessions = getAllSessions();
		if(sessions!=null){
			sessions.stream().forEach(f->f.unbindBackend(mySQLMetaBean, reason));
		}
	}
	
	public void addMySQLSession(MySQLMetaBean mySQLMetaBean, MySQLSession mySQLSession) {
		LinkedList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(mySQLMetaBean);
		if (mySQLSessionList == null) {
			mySQLSessionList = new LinkedList<>();
			mySQLSessionMap.put(mySQLMetaBean, mySQLSessionList);
		}
		mySQLSession.proxyBuffer.reset();  //想reactor 中放入mysqlession 时，需要做一次reset
		mySQLSessionList.add(mySQLSession);
	}
	
	/**
	 * 统计后端正在使用的连接数
     */
	private int getUsingBackendConCounts(MySQLMetaBean mySQLMetaBean) {
		return allSessions.stream()
//				.filter(session -> session instanceof MycatSession)
				.map(session -> {
					MycatSession mycatSession = (MycatSession) session;
					return mycatSession.getBackendConCounts(mySQLMetaBean);
				})
				.reduce(0, (sum, count) -> sum += count, (sum1, sum2) -> sum1 + sum2);
	}
	
	
	public void createSession(MySQLMetaBean mySQLMetaBean, SchemaBean schema, AsynTaskCallBack<MySQLSession> callBack) throws IOException {
		int count = Stream.of(ProxyRuntime.INSTANCE.getReactorThreads())
						.map(session -> ((MycatReactorThread)session).mySQLSessionMap.get(mySQLMetaBean))
						.filter(list -> list != null)
						.reduce(0, (sum, list) -> sum += list.size(), (sum1, sum2) -> sum1 + sum2);
		int backendCounts = getUsingBackendConCounts(mySQLMetaBean);
		logger.debug("all session backend count is {},reactor backend count is {},metabean max con is {}",backendCounts,count,mySQLMetaBean.getDsMetaBean().getMaxCon());
		if (count + backendCounts + 1 > mySQLMetaBean.getDsMetaBean().getMaxCon()) {
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno  = ErrorCode.ER_UNKNOWN_ERROR;
			errPkg.message = "backend connection is full for " + mySQLMetaBean.getDsMetaBean().getIp() + ":" + mySQLMetaBean.getDsMetaBean().getPort();
			callBack.finished(null, null, false, errPkg);
			return;
		}
		try {
			new BackendConCreateTask(bufPool, selector, mySQLMetaBean, schema,callBack);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 从当前reactor中获取连接
     * 3. reactor thread中空闲的backend
	 * 4. 连接池中的 backend
	 * 5. 是否可以新建连接
	 * @return
	 * @throws IOException 
	 */
	public void getMySQLSession(MycatSession currMycatSession,boolean runOnSlave,MySQLMetaBean targetMetaBean,AsynTaskCallBack<MySQLSession> callback) throws IOException {
		
		MySQLSession mysqlSession = null;
		// 3. 从当前ator 其他mycatSession 中获取连接
		LinkedList<MycatSession> mycatSessions = getAllSessions();
		mysqlSession = mycatSessions.stream()
				.filter(f -> !f.equals(currMycatSession))
                .map(mycatSession->mycatSession.getCurrCachedSession(targetMetaBean, runOnSlave,true))
                .filter(session -> session != null).findFirst().orElse(null);
        if (mysqlSession != null) {
			logger.debug("Use reactor cached backend connections for {}.{}:{}",
					(runOnSlave ? "read" : "write"),
					mysqlSession.getMySQLMetaBean().getDsMetaBean().getIp(),
					mysqlSession.getMySQLMetaBean().getDsMetaBean().getPort());
            mysqlSession.getMycatSession().unbindBeckend(mysqlSession);
            currMycatSession.bindBackend(mysqlSession);
            syncAndExecute(mysqlSession,callback);
            return;
        }

        //4. 从ds中获取已经建立的连接

		LinkedList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(targetMetaBean);
		if (mySQLSessionList != null && !mySQLSessionList.isEmpty()) {
			mysqlSession = mySQLSessionList.removeLast();
			if(mysqlSession!=null && mysqlSession.isIDLE()){
				logger.debug("Using the existing session in the datasource  for {}. {}:{}",
						(runOnSlave ? "read" : "write"),
						mysqlSession.getMySQLMetaBean().getDsMetaBean().getIp(),
						mysqlSession.getMySQLMetaBean().getDsMetaBean().getPort());
				currMycatSession.bindBackend(mysqlSession);
				syncAndExecute(mysqlSession,callback);
				return;
			}
		}
		
        // 5. 新建连接
		if(logger.isDebugEnabled()){
			logger.debug("create new connection for "+(runOnSlave?"read":"write"));
		}
    	
		createSession(targetMetaBean, currMycatSession.schema, (optSession, Sender, exeSucces, retVal) -> {

			//恢复默认的Handler
			currMycatSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
			if (exeSucces) {
				//设置当前连接 读写分离属性
				optSession.setDefaultChannelRead(targetMetaBean.isSlaveNode());
				optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
				currMycatSession.bindBackend(optSession);
				syncAndExecute(optSession,callback);
//				addMySQLSession(targetMetaBean, optSession); //新创建的连接加入到当前reactor 中
			} else {
				if(retVal instanceof ErrorPacket){
					currMycatSession.responseOKOrError((ErrorPacket)retVal);
				}else{
					System.err.println(" retVal is not ErrorPacket, please check it !!!");
					ErrorPacket error = new ErrorPacket();
		            error.errno = ErrorCode.ER_UNKNOWN_ERROR;
		            error.packetId = 1;
		            error.message = retVal.toString();
		            currMycatSession.responseOKOrError(error);
				}
			}
		});
	}
	
	/**
	 *  用于心跳 时，获取可用连接
	 * @param mySQLMetaBean
	 * @param callback
	 * @throws IOException
	 */
	public void getMysqlSession(MySQLMetaBean mySQLMetaBean,AsynTaskCallBack<MySQLSession> callback) throws IOException{
		MySQLSession mysqlSession = null;
		// 3. 从当前ator 其他mycatSession 中获取连接
		LinkedList<MycatSession> mycatSessions = getAllSessions();
		mysqlSession = mycatSessions.stream()
                .map(mycatSession->mycatSession.getMySQLSession(mySQLMetaBean))
                .filter(session -> session != null).findFirst().orElse(null);
        if (mysqlSession != null) {
        	if (logger.isDebugEnabled()) {
    			logger.debug("Use front sessionMap cached backend connections.{}",mysqlSession);
    		}
        	
        	mysqlSession.getMycatSession().unbindBeckend(mysqlSession);
            callback.finished(mysqlSession, null, true, null);
            return;
        }
        
        //4. 从ds中获取已经建立的连接
        LinkedList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(mySQLMetaBean);
  		if (mySQLSessionList != null && !mySQLSessionList.isEmpty()) {
  			mysqlSession = mySQLSessionList.removeLast();
  			if(mysqlSession!=null){
  				if(logger.isDebugEnabled()){
  					logger.debug("Using the existing session in the datasource .{} \n {}",mysqlSession.getMySQLMetaBean(),mysqlSession);
  				}
  				callback.finished(mysqlSession, null, true, null);
  				return;
  			}
  		}
  		
          // 5. 新建连接
  		if(logger.isDebugEnabled()){
  			logger.debug("create new connection ");
  		}
      	
  		createSession(mySQLMetaBean, null, (optSession, Sender, exeSucces, retVal) -> {

  			if (exeSucces) {
  	  			//设置当前连接 读写分离属性
  	  			optSession.setDefaultChannelRead(mySQLMetaBean.isSlaveNode());
  	  			//恢复默认的Handler
  	  			optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
  				callback.finished(optSession, null, true, null);
  			} else {
  				callback.finished(optSession, null, false, retVal);
  			}
  		});
	}
	
	/**
	 * 同步后端连接状态
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	public void syncAndExecute(MySQLSession mysqlSession,AsynTaskCallBack<MySQLSession> callback) throws IOException {
		MycatSession mycatSession = mysqlSession.getMycatSession();
		BackendSynchronzationTask backendSynchronzationTask = new BackendSynchronzationTask(mycatSession,mysqlSession);
		backendSynchronzationTask.setCallback((optSession, sender, exeSucces, rv) -> {
			//恢复默认的Handler
			mycatSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
			optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
			if (exeSucces) {
				syncSchemaToBackend(optSession,callback);
			} else {
				ErrorPacket errPkg = (ErrorPacket) rv;
				mycatSession.close(true, errPkg.message);
			}
		});
		backendSynchronzationTask.syncState(mycatSession, mysqlSession);
//		mycatSession.setCurNIOHandler(backendSynchronzationTask);
	}

	/**
	 * 同步 schema 到后端
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	public void syncSchemaToBackend(MySQLSession mysqlSession,AsynTaskCallBack<MySQLSession> callback)  throws IOException{
		if(mysqlSession.getMycatSession().schema!=null
				&&!mysqlSession.getMycatSession().schema.getDefaultDN().getDatabase().equals(mysqlSession.getDatabase())){
			MycatSession mycatSession = mysqlSession.getMycatSession();
			BackendSynchemaTask backendSynchemaTask = new BackendSynchemaTask(mysqlSession);
			backendSynchemaTask.setCallback((optSession, sender, exeSucces, rv) -> {
				//恢复默认的Handler
				mycatSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
				optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
				if (exeSucces) {
					if(callback!=null){
						callback.finished(optSession, sender, exeSucces, rv);
					}
				} else {
					ErrorPacket errPkg = (ErrorPacket) rv;
					mycatSession.responseOKOrError(errPkg);
				}
			});
			mycatSession.setCurNIOHandler(backendSynchemaTask);
		}else{
			if(callback!=null){
				callback.finished(mysqlSession, null, true, null);
			}
		}
	}
}
