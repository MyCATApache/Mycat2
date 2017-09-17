package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.OKPacket;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class BlockSqlCmd implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(BlockSqlCmd.class);

	public static final BlockSqlCmd INSTANCE = new BlockSqlCmd();

	

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		logger.debug("current buffer is "+session.proxyBuffer);
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		if(session.sqlContext.getSQLType() == NewSQLContext.SHOW_SQL) {
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno  = 1049;
			errPkg.message = "not support sql show";
			session.proxyBuffer.reset();
			session.curSQLCommand = this;
			session.responseOKOrError(errPkg);
			logger.debug(errPkg.message);
			return true;
		}
		return false;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {

		return true;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		session.proxyBuffer.flip();
		// session.chnageBothReadOpts();
		session.takeOwner(SelectionKey.OP_READ);
		return true;
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

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
	}

	@Override
	public void clearBackendResouces(MySQLSession mysqlSession, boolean sessionCLosed) {
	}
}
