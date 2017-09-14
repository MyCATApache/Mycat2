package io.mycat.mycat2.tasks;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.CommandPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;

public class BackendSynchemaTask extends AbstractBackendIOTask<MySQLSession> {
	
	private static Logger logger = LoggerFactory.getLogger(BackendSynchemaTask.class);
	
	public BackendSynchemaTask(MySQLSession session) throws IOException{
		super(session,true);
		session.proxyBuffer.reset();
		CommandPacket packet = new CommandPacket();
		packet.packetId = 0;
		packet.command = MySQLPacket.COM_INIT_DB;
		packet.arg = session.getMycatSession().schema.getDefaultDN().getDatabase().getBytes();
		packet.write(session.proxyBuffer);
		session.proxyBuffer.flip();
		session.proxyBuffer.readIndex = session.proxyBuffer.writeIndex;
        session.writeToChannel();
	}

	@Override
	public void onSocketRead(MySQLSession session) throws IOException {
		session.proxyBuffer.reset();
        if (!session.readFromChannel()) {// 没有读到数据或者报文不完整
            return;
        }
		
    	switch (session.resolveMySQLPackage(session.proxyBuffer, session.curMSQLPackgInf, true)) {
		case Full:
			if(session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET){
				String database = session.getMycatSession().schema.getDefaultDN().getDatabase();
				session.setDatabase(database );
				this.finished(true);
			}else if(session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET){
				 errPkg = new ErrorPacket();
	            errPkg.read(session.proxyBuffer);
	            logger.warn("backend state sync Error.Err No. " + errPkg.errno + "," + errPkg.message);
	            this.finished(false);
			}
			break;
		default:
			return;
    	}
	}

}
