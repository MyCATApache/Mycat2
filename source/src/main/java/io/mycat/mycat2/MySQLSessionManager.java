package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL Session Manager (bakcend mysql connection manager)
 * 
 * @author wuzhihui
 *
 */
public class MySQLSessionManager implements SessionManager<MySQLSession> {
	protected static Logger logger = LoggerFactory.getLogger(MySQLSessionManager.class);
	protected Map<MySQLMetaBean, ArrayList<MySQLSession>> mySQLSessionMap = new HashMap<>();

	@Override
	public MySQLSession createSession(Object keyAttachment, BufferPool bufPool, Selector nioSelector,
			SocketChannel serverChanel) throws IOException {
		MySQLSession session = new MySQLSession(bufPool, nioSelector, serverChanel);
		MySQLMetaBean mySQLMetaBean = (MySQLMetaBean) keyAttachment;
		session.setMySQLMetaBean(mySQLMetaBean);
		session.setSessionManager(this);
		addNewMySQLSession(session);
		return session;
	}

	public ArrayList<MySQLSession> getSessionsOfHost(MySQLMetaBean mysqlMetaBean) {
		return mySQLSessionMap.get(mysqlMetaBean);
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
	 * 异步方式创建一个MySQL连接，成功或失败，都通过callBack回调通知给用户逻辑
	 * @param mySQLMetaBean 后端MySQL的信息
	 * @param schema
	 * @param callBack 回调接口
	 * @throws IOException
	 */
	public void createSession(MySQLMetaBean mySQLMetaBean, SchemaBean schema, AsynTaskCallBack<MySQLSession> callBack)
			throws IOException {
		int backendCounts = 0;
		for (MycatReactorThread reActorthread : ProxyRuntime.INSTANCE.getMycatReactorThreads()) {
			ArrayList<MySQLSession> list = reActorthread.mysqlSessionMan.getSessionsOfHost(mySQLMetaBean);
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
		MycatReactorThread curThread = (MycatReactorThread) Thread.currentThread();
		try {
			new BackendConCreateTask(curThread.getBufPool(), curThread.getSelector(), mySQLMetaBean, schema, callBack);
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

	private void addNewMySQLSession(MySQLSession mySQLSession) {
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

	@Override
	public Collection<MySQLSession> getAllSessions() {
		Collection<MySQLSession> result = new ArrayList<>();
		for (ArrayList<MySQLSession> sesLst : this.mySQLSessionMap.values()) {
			result.addAll(sesLst);
		}

		return result;
	}

	public void removeSession(MySQLSession theSession) {
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

	@Override
	public NIOHandler<MySQLSession> getDefaultSessionHandler() {
		return MainMySQLNIOHandler.INSTANCE;
	}

	@Override
	public int curSessionCount() {
		int count = 0;
		for (ArrayList<MySQLSession> sesLst : this.mySQLSessionMap.values()) {
			count += sesLst.size();
		}
		return count;
	}

}
