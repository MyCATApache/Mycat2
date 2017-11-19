package io.mycat.mycat2.cmds.sqlCmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mysql.packet.OKPacket;

/**
 * This statement stops the MySQL server
 * @author yanjunli
 *
 */
public class SqlComShutdownCmd extends DirectPassthrouhCmd{
	
	private static final Logger logger = LoggerFactory.getLogger(SqlComShutdownCmd.class);

	public static final SqlComShutdownCmd INSTANCE = new SqlComShutdownCmd();
	
	private SqlComShutdownCmd(){}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		session.responseOKOrError(OKPacket.OK);
		
        return false;
	}
	
	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		logger.warn("mycat exit. bye");
		System.exit(0);
		return true;
	}

}
