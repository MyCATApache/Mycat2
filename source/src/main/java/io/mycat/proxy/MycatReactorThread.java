package io.mycat.proxy;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MySQLSessionManager;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendSynchemaTask;
import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import io.mycat.proxy.buffer.BufferPool;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * mycat 多个Session会话
 *
 * @author yanjunli
 * @author Leader us
 */
public class MycatReactorThread extends ProxyReactorThread<MycatSession> {

	protected final static Logger logger = LoggerFactory.getLogger(MycatReactorThread.class);

	/**
	 * 每个Reactor独立的一个后端连接池不共享（在MySQLSessionManager中记录），每个Reactor线程自己负责清理释放多余富足的连接，确保
	 * 所有Reactor线程拥有的后端连接数总和为全局连接池总数
	 */
	public final MySQLSessionManager mysqlSessionMan = new MySQLSessionManager();

	public MycatReactorThread(BufferPool bufPool) throws IOException {
		super(bufPool);
	}

	/**
	 * 从当前reactor中获取连接 3. reactor thread中空闲的backend 4. 连接池中的 backend 5. 是否可以新建连接
	 *
	 * @return
	 * @throws IOException
	 */
	public void tryGetMySQLAndExecute(MycatSession currMycatSession, boolean runOnSlave, MySQLMetaBean targetMetaBean,
			AsynTaskCallBack<MySQLSession> callback) throws IOException {
		if (Thread.currentThread() != this) {
			throw new RuntimeException("Not in current MycatReactorThread");
		}
		logger.debug("tryGetMySQLAndExecute  on mysql {} for callback {} ",targetMetaBean,callback);
		// 从ds中获取已经建立的连接
		ArrayList<MySQLSession> mySQLSessionList = mysqlSessionMan.getSessionsOfHost(targetMetaBean);
		if (mySQLSessionList != null && !mySQLSessionList.isEmpty()) {
			for (MySQLSession mysqlSession : mySQLSessionList) {
				if (mysqlSession.isIdle()) {
					logger.debug("Using the existing session in the datasource  for {}. {}:{}",
							(runOnSlave ? "read" : "write"), mysqlSession.getMySQLMetaBean().getDsMetaBean().getIp(),
							mysqlSession.getMySQLMetaBean().getDsMetaBean().getPort());
					MycatSession oldMycatSession = mysqlSession.getMycatSession();
					if (oldMycatSession != null) {
						oldMycatSession.unbindBackend(mysqlSession);
					}
					currMycatSession.bindBackend(mysqlSession);
					syncAndExecute(mysqlSession, callback);
					return;
				}
			}
		}

		// 新建连接
		if (logger.isDebugEnabled()) {
			logger.debug("create new connection for " + (runOnSlave ? "read" : "write"));
		}

		mysqlSessionMan.createSession(targetMetaBean, currMycatSession.mycatSchema,
				(optSession, Sender, exeSucces, retVal) -> {
					// 恢复默认的Handler
					currMycatSession.setCurNIOHandler(MainMycatNIOHandler.INSTANCE);
					if (exeSucces) {
						// 设置当前连接 读写分离属性
						optSession.setDefaultChannelRead(targetMetaBean.isSlaveNode());
						optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
						currMycatSession.bindBackend(optSession);
						syncAndExecute(optSession, callback);
						// addMySQLSession(targetMetaBean, optSession);
					} else {
						callback.finished(optSession, Sender, exeSucces, retVal);
					}
				});
	}

	/**
	 * 用于心跳 时，获取可用连接
	 *
	 * @param mySQLMetaBean
	 * @param callback
	 * @throws IOException
	 */
	public void getHeatBeatCon(MySQLMetaBean mySQLMetaBean, AsynTaskCallBack<MySQLSession> callback)
			throws IOException {
		if (Thread.currentThread() != this) {
			throw new RuntimeException("Not in current MycatReactorThread");
		}
		// 从ds中获取已经建立的连接
		ArrayList<MySQLSession> mySQLSessionList = this.mysqlSessionMan.getSessionsOfHost(mySQLMetaBean);
		if (mySQLSessionList != null && !mySQLSessionList.isEmpty()) {
			for (MySQLSession mysqlSession : mySQLSessionList) {
				if (mysqlSession.isIdle()) {
					logger.debug("Using the existing session in the datasource  for heart beat. {}:{}",
							mysqlSession.getMySQLMetaBean().getDsMetaBean().getIp(),
							mysqlSession.getMySQLMetaBean().getDsMetaBean().getPort());
					mysqlSession.getMycatSession().unbindBackend(mysqlSession);
					mysqlSession.setIdle(false);
					callback.finished(mysqlSession, null, true, null);
					return;
				}
			}
		}

		// 新建连接
		if (logger.isDebugEnabled()) {
			logger.debug("create new connection for heartbeat.");
		}
		mysqlSessionMan.createSession(mySQLMetaBean, null, (optSession, Sender, exeSucces, retVal) -> {
			if (exeSucces) {
				// 设置当前连接 读写分离属性
				optSession.setDefaultChannelRead(mySQLMetaBean.isSlaveNode());
				// 恢复默认的Handler
				optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
				callback.finished(optSession, null, true, null);
			} else {
				callback.finished(optSession, null, false, retVal);
			}
		});
	}

	/**
	 * 同步后端连接状态
	 *
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	public void syncAndExecute(MySQLSession mysqlSession, AsynTaskCallBack<MySQLSession> callback) throws IOException {
		MycatSession mycatSession = mysqlSession.getMycatSession();
		BackendSynchronzationTask backendSynchronzationTask = new BackendSynchronzationTask(mycatSession, mysqlSession);
		backendSynchronzationTask.setCallback((optSession, sender, exeSucces, rv) -> {
			// 恢复默认的Handler
			mycatSession.setCurNIOHandler(MainMycatNIOHandler.INSTANCE);
			optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
			if (exeSucces) {
				syncSchemaToBackend(optSession, callback);
			} else {
				// ErrorPacket errPkg = (ErrorPacket) rv;
				// mycatSession.close(true, errPkg.message);
				callback.finished(optSession, sender, exeSucces, rv);
			}
		});
		backendSynchronzationTask.syncState(mycatSession, mysqlSession);
		// mycatSession.setCurNIOHandler(backendSynchronzationTask);
	}

	/**
	 * 同步 mycatSchema 到后端
	 *
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	public void syncSchemaToBackend(MySQLSession mysqlSession, AsynTaskCallBack<MySQLSession> callback)
			throws IOException {
		if (StringUtils.isEmpty(mysqlSession.getDatabase())) {
			MycatSession mycatSession = mysqlSession.getMycatSession();
			BackendSynchemaTask backendSynchemaTask = new BackendSynchemaTask(mysqlSession,(optSession, sender, exeSucces, rv) -> {
				// 恢复默认的Handler
				mycatSession.setCurNIOHandler(MainMycatNIOHandler.INSTANCE);
				optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
				callback.finished(optSession, sender, exeSucces, rv);
			});
			mycatSession.setCurNIOHandler(backendSynchemaTask);
		} else {
			callback.finished(mysqlSession, null, true, null);
		}
	}
}
