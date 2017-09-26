package io.mycat.mycat2.cmds.manager;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.CheckResult;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyRuntime;
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
		
		MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
		MySQLRepBean repBean = conf.getMySQLRepBean(replName);
		CheckResult result = repBean.switchDataSourcecheck(index);

		if(!result.isSuccess()){
			ErrorPacket errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno  = ErrorCode.ERR_WRONG_USED;
			errPkg.message = result.getMsg();
			session.responseOKOrError(errPkg);
		}else{
			ProxyRuntime.INSTANCE.prepareSwitchDataSource(replName, index,false);
			OKPacket packet = new OKPacket();
	        packet.packetId = 1;
	        packet.affectedRows = 1;
	        packet.serverStatus = 2;
	        session.responseOKOrError(packet);
		}
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
