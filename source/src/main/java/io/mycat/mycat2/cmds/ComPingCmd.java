package io.mycat.mycat2.cmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.packet.OKPacket;

/**
 * COM_PING: check if the server is alive Returns OK_Packet Payload 1 [0e]
 * COM_PING
 * 
 * @author yanjunli
 *
 */
public class ComPingCmd implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(ComPingCmd.class);

	public static final ComPingCmd INSTANCE = new ComPingCmd();

	private ComPingCmd() {
	}

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		session.responseOKOrError(OKPacket.DEFAULT_OK_PACKET);
		return true;
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearResouces(MycatSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub

	}

}
