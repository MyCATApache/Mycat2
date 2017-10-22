package io.mycat.mycat2.cmds.interceptor;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.util.ErrorCode;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class BlockSqlCmd extends SQLAnnotationCmd {

	private static final Logger logger = LoggerFactory.getLogger(BlockSqlCmd.class);

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno  = ErrorCode.ERR_WRONG_USED;
			errPkg.message = getErrMsg();
			session.proxyBuffer.reset();
			session.responseOKOrError(errPkg);
			logger.error(errPkg.message);
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		session.proxyBuffer.flip();
		// session.chnageBothReadOpts();
		session.takeOwner(SelectionKey.OP_READ);
		return true;
	}
}
