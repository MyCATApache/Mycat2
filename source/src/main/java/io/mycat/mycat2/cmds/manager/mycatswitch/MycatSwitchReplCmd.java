package io.mycat.mycat2.cmds.manager.mycatswitch;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.CheckResult;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ErrorCode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.regex.Pattern;
/**
 * 主从复制组切换命令
 * @author yanjunli
 *
 */
public class MycatSwitchReplCmd implements MySQLCommand {
	
	private static final Logger logger = LoggerFactory.getLogger(MycatSwitchReplCmd.class);

	public static final MycatSwitchReplCmd INSTANCE = new MycatSwitchReplCmd();
		
	private final static String splitStr = "(?!^[\\s]*)\\s+(?![\\s]*$)";

    private final static Pattern splitPattern = Pattern.compile(splitStr);
	
	private MycatSwitchReplCmd(){}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		String executeSql = session.sqlContext.getRealSQL(0);

        String[] params = splitPattern.split(executeSql);
		
		if(params.length!=5){
			session.sendErrorMsg(ErrorCode.ERR_WRONG_USED," Invalid number of parameters.");
			return false;
		}
		
		if(!StringUtils.isNumeric(params[4].trim())){
			session.sendErrorMsg(ErrorCode.ERR_WRONG_USED," Invalid type of parameter ["+params[1]+"].");
			return false;
		}
		
		String replName = params[3];
		int index = Integer.valueOf(params[4]);
		
		MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
		MySQLRepBean repBean = conf.getMySQLRepBean(replName);
		
		if(null==repBean){
			session.sendErrorMsg(ErrorCode.ERR_WRONG_USED," Invalid replName ["+replName+"].");
			return false;
		}
		
		CheckResult result = repBean.switchDataSourcecheck(index);

		if(!result.isSuccess()){
			session.sendErrorMsg(ErrorCode.ERR_WRONG_USED,result.getMsg());
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
    public boolean onBackendResponse(MySQLSession session) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		session.proxyBuffer.flip();
		session.takeOwner(SelectionKey.OP_READ);
		return false;
	}

	@Override
    public boolean onBackendWriteFinished(MySQLSession session) {
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
