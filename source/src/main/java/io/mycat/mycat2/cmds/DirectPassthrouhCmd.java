package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class DirectPassthrouhCmd implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);

	public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		session.clearReadWriteOpts();

		session.getBackend((mysqlsession, sender, success, result) -> {

			ProxyBuffer curBuffer = session.proxyBuffer;
			// 切换 buffer 读写状态
			curBuffer.flip();
			if (success) {
				// 没有读取,直接透传时,需要指定 透传的数据 截止位置
				curBuffer.readIndex = curBuffer.writeIndex;
				// 改变 owner，对端Session获取，并且感兴趣写事件
				session.giveupOwner(SelectionKey.OP_WRITE);
				try {
					mysqlsession.writeToChannel();
				} catch (IOException e) {
					session.closeBackendAndResponseError(mysqlsession, success, ((ErrorPacket) result));
				}
			} else {
				session.closeBackendAndResponseError(mysqlsession, success, ((ErrorPacket) result));
			}
		});
		return false;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {

		// 首先进行一次报文的读取操作
		if (!session.readFromChannel()) {
			return false;
		}

		// 进行报文处理的流程化
		boolean nextReadFlag = false;
		do {
			// 进行报文的处理流程
			nextReadFlag = session.getMycatSession().commandHandler.procss(session);
		} while (nextReadFlag);

		// 获取当前是否结束标识
		Boolean check = (Boolean) session.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());

		MycatSession mycatSession = session.getMycatSession();
		ProxyBuffer buffer = session.getProxyBuffer();

		buffer.flip();
		// 检查到当前已经完成,执行添加操作
		if (null != check && check) {
			// 当知道操作完成后，前段的注册感兴趣事件为读取
			mycatSession.takeOwner(SelectionKey.OP_READ);
		}
		// 未完成执行继续读取操作
		else {
			// 直接透传报文
			mycatSession.takeOwner(SelectionKey.OP_WRITE);
		}

		mycatSession.writeToChannel();

		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		// 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
		// 检查当前已经结束，进行切换
		// 检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
		if (session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey())) {
			session.proxyBuffer.flip();
			session.giveupOwner(SelectionKey.OP_READ);
			return false;
		}
		// 当传输标识不存在，则说已经结束，则切换到前端的读取
		else {
			session.proxyBuffer.flip();
			// session.chnageBothReadOpts();
			session.takeOwner(SelectionKey.OP_READ);
			return true;
		}
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) {
		// 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
		// 向后端写入完成数据后，则从后端读取数据
		session.proxyBuffer.flip();
		// 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
		session.change2ReadOpts();
		return false;

	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) {

		return true;
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		if (sessionCLosed) {
			session.recycleAllocedBuffer(session.getProxyBuffer());
			session.unbindAllBackend();
		}
	}

	@Override
	public void clearBackendResouces(MySQLSession mysqlSession, boolean sessionCLosed) {
		if (sessionCLosed) {
			mysqlSession.recycleAllocedBuffer(mysqlSession.getProxyBuffer());
		}
	}
}
