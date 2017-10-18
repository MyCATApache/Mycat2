package io.mycat.mycat2.cmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.util.ErrorCode;
import io.mycat.util.ParseUtil;

public class NotSupportCmd implements MySQLCommand{
	
	private static final Logger logger = LoggerFactory.getLogger(NotSupportCmd.class);

	public static final NotSupportCmd INSTANCE = new NotSupportCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		ErrorPacket error = new ErrorPacket();
        error.errno = ErrorCode.ER_BAD_DB_ERROR;
        error.packetId = session.proxyBuffer.getByte(session.curMSQLPackgInf.startPos 
				+ ParseUtil.mysql_packetHeader_length);
        error.message = " command  is not supported";
        session.responseOKOrError(error);
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
		// TODO Auto-generated method stub
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
