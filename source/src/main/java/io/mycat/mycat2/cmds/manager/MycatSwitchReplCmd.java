package io.mycat.mycat2.cmds.manager;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.util.ErrorCode;
/**
 * 主从复制组切换命令
 * @author yanjunli
 *
 */
public class MycatSwitchReplCmd implements MySQLCommand {
	
	private static final Logger logger = LoggerFactory.getLogger(MycatSwitchReplCmd.class);

	public static final MycatSwitchReplCmd INSTANCE = new MycatSwitchReplCmd();
	
	private MycatSwitchReplCmd(){}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		String charset = session.charSet.charset==null?StandardCharsets.UTF_8.name():session.charSet.charset;
		HashArray hashArray = session.sqlContext.getMyCmdValue();
		String replName = new String(TokenizerUtil.pickBytes(0, hashArray, session.proxyBuffer),charset);
		int index = TokenizerUtil.pickNumber(1, hashArray, session.proxyBuffer);
		/*
		 * TODO 暂时打印 错误 报文, 等待与 智文 整合。整合时 需要删除 下面返回的错误报文逻辑
		 */
		ErrorPacket errPkg = new ErrorPacket();
		errPkg.packetId = 1;
		errPkg.errno  = ErrorCode.ERR_WRONG_USED;
		errPkg.message = " the command is mycat switch repl "+replName+" " + index;
		session.proxyBuffer.reset();
		session.responseOKOrError(errPkg);
		
		return false;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		session.proxyBuffer.flip();
		// session.chnageBothReadOpts();
		session.takeOwner(SelectionKey.OP_READ);
		return false;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub

	}
}
