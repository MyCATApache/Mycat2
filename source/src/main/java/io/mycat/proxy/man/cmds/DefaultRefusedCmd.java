package io.mycat.proxy.man.cmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ManErrorCode;
import io.mycat.proxy.man.packet.FailedPacket;

/**
 * Server端默认拒绝执行命令的Command
 * 
 * @author wuzhihui
 *
 */
public class DefaultRefusedCmd implements AdminCommand {
	protected static Logger logger = LoggerFactory.getLogger(DefaultRefusedCmd.class);

	@Override
	public void handlerPkg(AdminSession session) throws IOException {
		String errMsg = "not implemented this package ,Maybe a bug ,Leader us want you  fix it ,pkg type :"
				+ session.frontBuffer.getByte(2);
		logger.warn(errMsg);
		FailedPacket failedPkg = new FailedPacket(ManErrorCode.MAYBE_BUG_ERROR, errMsg);
		session.answerClientNow(failedPkg);

	}

}
