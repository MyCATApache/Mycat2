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
import io.mycat.util.ErrorCode;

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
		 * 
		 */
		if(session.sqlContext.getSQLType() == NewSQLContext.ALTER_SQL) {
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno  = ErrorCode.ERR_NOT_SUPPORTED;
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
