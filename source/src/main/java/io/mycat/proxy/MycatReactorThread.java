package io.mycat.proxy;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mycat2.tasks.BackendSynchemaTask;
import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ErrorCode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * mycat 多个Session会话
 *
 * @author yanjunli
 * @author Leader us
 */
public class MycatReactorThread extends ProxyReactorThread<MycatSession> {

	protected final static Logger logger = LoggerFactory.getLogger(MycatReactorThread.class);

	/**
	 * 存放后端连接的map,每个Reactor独立的一个后端连接池，不共享，每个Reactor线程自己负责清理释放多余富足的连接，确保
	 * 所有Reactor线程拥有的后端连接数总和为全局连接池总数 注意：这里保存了当前MycatReactorThread对象所拥有的所有后端连接。
	 */
	protected Map<MySQLMetaBean, ArrayList<MySQLSession>> mySQLSessionMap = new HashMap<>();

	public MycatReactorThread(BufferPool bufPool) throws IOException {
		super(bufPool);
	}

	public void removeMySQLSessionFromMap(MySQLSession theSession) {
		ArrayList<MySQLSession> mysqlSessions = mySQLSessionMap.get(theSession.getMySQLMetaBean());
		boolean find = false;
		if (mysqlSessions != null && mysqlSessions.remove(theSession)) {
			find = true;
		}

		if (!find) {
			logger.warn("can't find MySQLSession  in map ,It's a bug ,please fix it ,{}", theSession);
		} else {
			logger.debug("removed MySQLSession  from  map .{} ", theSession);
		}
	}

	/**
	 * 清理DatasourceMetaBean相关的所有MySQL连接（关闭）
	 *
	 * @param dsMetaBean
	 * @param reason
	 */
	public void clearAndDestroyMySQLSession(MySQLMetaBean dsMetaBean, String reason) {
		ArrayList<MySQLSession> mysqlSessions = mySQLSessionMap.get(dsMetaBean);
		if (mysqlSessions != null) {
			mysqlSessions.forEach(f -> {
				// 被某个Mycat连接所使用，则同时关闭Mycat连接
				if (f.getMycatSession() != null && !f.isIdle()) {
					logger.info("close Mycat session ,for it's using MySQL Con {} ", f);
					f.getMycatSession().close(false, reason);
				}
				// 关闭MySQL连接
				f.close(false, reason);
			});
			// 清空MySQL连接池
			mysqlSessions.clear();
			mySQLSessionMap.remove(dsMetaBean);

		}
	}

	/**
	 * 把新创建成功的MySQL连接放入到本Reactor所在的连接池中，注意，要避免重复放入，需要认真检查是否在其他地方已经放入，
	 * 通过BackendConCreateTask方式创建的连接，有参数控制是否放入
	 *
	 * @param mySQLSession
	 *            成功建立的的MySQL连接
	 */
	public void addNewMySQLSession(MySQLSession mySQLSession) {
		ArrayList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(mySQLSession.getMySQLMetaBean());
		if (mySQLSessionList == null) {
			mySQLSessionList = new ArrayList<>(50);
			if (null != mySQLSessionMap.putIfAbsent(mySQLSession.getMySQLMetaBean(), mySQLSessionList)) {
				throw new RuntimeException(
						"Duplicated MySQL Session ！！！，Please fix this Bug! Leader call you ! " + mySQLSession);
			}
		}
		mySQLSession.proxyBuffer.reset();
		mySQLSessionList.add(mySQLSession);
	}

	public void createSession(MySQLMetaBean mySQLMetaBean, SchemaBean schema, AsynTaskCallBack<MySQLSession> callBack)
			throws IOException {
		int backendCounts = 0;
		for (ProxyReactorThread<?> thread : ProxyRuntime.INSTANCE.getReactorThreads()) {
			ArrayList<MySQLSession> list = ((MycatReactorThread) thread).mySQLSessionMap.get(mySQLMetaBean);
			if (null != list) {
				backendCounts += list.size();
			}
		}
		if (backendCounts + 1 > mySQLMetaBean.getDsMetaBean().getMaxCon()) {
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
			errPkg.message = "backend connection is full for " + mySQLMetaBean.getDsMetaBean().getIp() + ":"
					+ mySQLMetaBean.getDsMetaBean().getPort();
			callBack.finished(null, null, false, errPkg);
			return;
		}
		try {
			new BackendConCreateTask(bufPool, selector, mySQLMetaBean, schema, callBack, true);
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage());
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
			errPkg.message = "failed to create backend connection task for " + mySQLMetaBean.getDsMetaBean().getIp()
					+ ":" + mySQLMetaBean.getDsMetaBean().getPort();
			callBack.finished(null, null, false, errPkg);
		}
	}

	/**
	 * 从当前reactor中获取连接 3. reactor thread中空闲的backend 4. 连接池中的 backend 5. 是否可以新建连接
	 *
	 * @return
	 * @throws IOException
	 */
	public void tryGetMySQLAndExecute(MycatSession currMycatSession, boolean runOnSlave, MySQLMetaBean targetMetaBean,
			AsynTaskCallBack<MySQLSession> callback) throws IOException {
		// 从ds中获取已经建立的连接

		ArrayList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(targetMetaBean);
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

		createSession(targetMetaBean, currMycatSession.mycatSchema, (optSession, Sender, exeSucces, retVal) -> {
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
		// 从ds中获取已经建立的连接
		ArrayList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(mySQLMetaBean);
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
		createSession(mySQLMetaBean, null, (optSession, Sender, exeSucces, retVal) -> {
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
			BackendSynchemaTask backendSynchemaTask = new BackendSynchemaTask(mysqlSession);
			backendSynchemaTask.setCallback((optSession, sender, exeSucces, rv) -> {
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
