package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mysql.MySQLPacketInf;
import io.mycat.mysql.PayloadType;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 直接透传命令报文
 *
 * @author wuzhihui
 */
public class DirectPassthrouhCmd implements MySQLCommand {

	public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();
	private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {

		MySQLSession curBackend = session.getCurBackend();
		if (curBackend != null) {
			if (session.getTargetDataNode() == null) {
				logger.warn("{} not specified SQL target DataNode ,so set to default dataNode ", session);
				DNBean targetDataNode = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap()
						.get(session.getMycatSchema().getDefaultDataNode());
				session.setTargetDataNode(targetDataNode);
			}
			if (curBackend.synchronizedState(session.getTargetDataNode().getDatabase())) {
				this.directTransetoBackend(session);
			} else {
				// 同步数据库连接状态后回调
				AsynTaskCallBack<MySQLSession> callback = (mysqlsession, sender, success, result) -> {
					ProxyBuffer curBuffer = session.proxyBuffer;
					// 切换 buffer 读写状态
					curBuffer.flip();
					if (success) {
						directTransetoBackend(session);
					} else {
						session.closeAllBackendsAndResponseError(success, ((ErrorPacket) result));
					}
				};
				curBackend.syncAndCallback(callback);
			}
		} else {// 没有当前连接，尝试获取新连接
			session.getBackendAndCallBack((mysqlsession, sender, success, result) -> {
				ProxyBuffer curBuffer = session.proxyBuffer;
				// 切换 buffer 读写状态
				curBuffer.flip();
				if (success) {
					// 没有读取,直接透传时,需要指定 透传的数据 截止位置
					curBuffer.readIndex = curBuffer.writeIndex;
					// 改变 owner，对端Session获取，并且感兴趣写事件
					session.giveupOwner(SelectionKey.OP_WRITE);
					mysqlsession.writeToChannel();
					mysqlsession.curPacketInf.shift2RespPacket();
					mysqlsession.curPacketInf.proxyBuffer = mysqlsession.proxyBuffer;
				} else {
					session.closeAllBackendsAndResponseError(success, ((ErrorPacket) result));
				}
			});
		}

		return false;
	}

	private void directTransetoBackend(MycatSession session) throws IOException {
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		session.clearReadWriteOpts();
		MySQLSession curBackend = session.getCurBackend();
		ProxyBuffer curBuffer = session.proxyBuffer;
		// 没有读取,直接透传时,需要指定 透传的数据 截止位置
		curBuffer.readIndex = curBuffer.writeIndex;
		// 改变 owner，对端Session获取，并且感兴趣写事件
        curBuffer.flip();
		session.giveupOwner(SelectionKey.OP_WRITE);
		curBackend.writeToChannel();
		curBackend.curPacketInf.shift2RespPacket();
		curBackend.curPacketInf.proxyBuffer = curBackend.proxyBuffer;
	}

	@Override
	public boolean onBackendResponse(MySQLSession mySQLSession) throws IOException {
		// 首先进行一次报文的读取操作
		if (!mySQLSession.readFromChannel()) {
			return false;
		}
		MySQLPacketInf packetInf = mySQLSession.curPacketInf;
		packetInf.proxyBuffer = mySQLSession.proxyBuffer;
		while (packetInf.needContinueResolveMySQLPacket()) {
			PayloadType payloadType = packetInf.resolveCrossBufferMySQLPayload(mySQLSession.proxyBuffer);
			StringUtil.print("onBackendResponse",payloadType, packetInf);
		}
		MycatSession mycatSession = mySQLSession.getMycatSession();
		ProxyBuffer buffer = mySQLSession.getProxyBuffer();
		buffer.flip();
		if (packetInf.isResponseFinished()) {
			mycatSession.takeOwner(SelectionKey.OP_READ);
			mySQLSession.curPacketInf.shift2DefRespPacket();
			mySQLSession.setIdle(!packetInf.isInteractive());
		} else {
			mycatSession.takeOwner(SelectionKey.OP_WRITE);
		}
		mycatSession.writeToChannel();
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession mycatSession) throws IOException {
		// 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
		// 检查当前已经结束，进行切换
		// 检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
		MySQLSession mySQLSession = mycatSession.getCurBackend();
		MySQLPacketInf mySQLPacketInf = mySQLSession.curPacketInf;
		if (!mySQLPacketInf.isResponseFinished()) {
			mycatSession.proxyBuffer.flip();
			mycatSession.giveupOwner(SelectionKey.OP_READ);
			return false;
		}
		// 当传输标识不存在，则说已经结束，则切换到前端的读取
		else {
			mycatSession.proxyBuffer.flip();
			mycatSession.takeOwner(SelectionKey.OP_READ);
			mycatSession.curPacketInf.shift2QueryPacket();
			return true;
		}
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession mySQLSession) {
		MycatSession mycatSession = mySQLSession.getMycatSession();
		MySQLPacketInf mycatPacketInf = mycatSession.curPacketInf;
		if (mycatPacketInf.needContinueOnReadingRequest()) {
			mycatSession.proxyBuffer.flip();
			mycatSession.takeOwner(SelectionKey.OP_READ);
			mycatPacketInf.shift2QueryPacket();
			return true;
		}
		// 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
		// 向后端写入完成数据后，则从后端读取数据
		mySQLSession.proxyBuffer.flip();
		// 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
		mySQLSession.change2ReadOpts();
		return false;

	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) {
		return true;
	}

	@Override
	public void clearResouces(MycatSession session, boolean sessionCLosed) {
		if (sessionCLosed) {
			session.recycleAllocedBuffer(session.getProxyBuffer());
			session.unbindBackends();
		}
	}

}
