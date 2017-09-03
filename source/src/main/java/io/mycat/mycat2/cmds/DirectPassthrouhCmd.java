package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.cmds.judge.DirectTransJudge;
import io.mycat.mycat2.cmds.judge.ErrorJudge;
import io.mycat.mycat2.cmds.judge.OkJudge;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class DirectPassthrouhCmd implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);

	public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();

	/**
	 * 指定需要处理的包类型信息
	 */
	private static final Map<Integer, DirectTransJudge> JUDGEMAP = new HashMap<>();

	static {
		// 用来进行ok包的处理理
		JUDGEMAP.put((int) MySQLPacket.OK_PACKET, OkJudge.INSTANCE);
		// 用来进行error包的处理
		JUDGEMAP.put((int) MySQLPacket.ERROR_PACKET, ErrorJudge.INSTANCE);
	}

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		session.getBackend((mysqlsession, sender, success,result)->{
			if(success){
				ProxyBuffer curBuffer = session.proxyBuffer;
				// 切换 buffer 读写状态
				curBuffer.flip();
				// 没有读取,直接透传时,需要指定 透传的数据 截止位置
				curBuffer.readIndex = curBuffer.writeIndex;
				// 改变 owner，对端Session获取，并且感兴趣写事件
				session.giveupOwner(SelectionKey.OP_WRITE);
				mysqlsession.writeToChannel();
			}
		});
		return false;
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {
		// TODO Auto-generated method stub

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
			nextReadFlag = session.currPkgProc.procssPkg(session);
		} while (nextReadFlag);

		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		// 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
		// 检查当前已经结束，进行切换
		logger.warn("not well implemented ,please fix it ");

		// 检查如果存在传输的标识，说明后传数据向前传传输未完成,注册后端的读取事件
		if (session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey())) {
			session.proxyBuffer.flip();
			session.giveupOwner(SelectionKey.OP_READ);
		}
		// 当传输标识不存在，则说已经结束，则切换到前端的读取
		else {
			session.proxyBuffer.flip();
			// session.chnageBothReadOpts();
			session.takeOwner(SelectionKey.OP_READ);
		}
		return false;

	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
		// 向后端写入完成数据后，则从后端读取数据
		session.proxyBuffer.flip();
		// 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
		session.change2ReadOpts();
		return false;

	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {

		return true;
	}

}
