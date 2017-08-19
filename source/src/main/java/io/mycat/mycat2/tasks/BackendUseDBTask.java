package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.INITDBPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * Created by ynfeng on 2017/8/13.
 * <p>
 * 同步状态至后端数据库，包括：字符集，事务，隔离级别等
 */
public class BackendUseDBTask extends AbstractBackendIOTask {
	private static Logger logger = LoggerFactory.getLogger(BackendUseDBTask.class);
	

	public BackendUseDBTask(MySQLSession session) throws IOException {
		super(session);
	    session.setCurNIOHandler(this);
		sendUseDB(session);
		
	}

	private void sendUseDB(MySQLSession session) throws IOException {
      INITDBPacket initDBPacket = new INITDBPacket();
      initDBPacket.sql =  session.schema.getDefaultDN().getDatabase();
      ProxyBuffer buffer = session.frontBuffer;
      buffer.reset();
      initDBPacket.write(buffer);
      buffer.flip();
      session.writeToChannel(buffer, session.backendChannel);
      //session.modifySelectKey();
	}

	@Override
	public void onBackendRead(MySQLSession session) throws IOException {
		session.frontBuffer.reset();
		if (!session.readFromChannel(session.frontBuffer, session.backendChannel)
				|| !session.resolveMySQLPackage(session.frontBuffer, session.curBackendMSQLPackgInf, false)) {// 没有读到数据或者报文不完整
			return;
		}
		ProxyBuffer curBuffer = session.frontBuffer;
        if(session.curBackendMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
          logger.debug("success set back connnection database, response ok to front");
          //session.backendKey.interestOps(SelectionKey.OP_READ);
          this.finished(true);
      } else {
          errPkg = new ErrorPacket();
          errPkg.read(session.frontBuffer);
          logger.warn("use DB fail " + session.schema.getDefaultDN().getDatabase());
          this.finished(false);
     
      }   
	}

	@Override
	public void onBackendSocketClosed(MySQLSession userSession, boolean normal) {
		logger.warn(" socket closed not handlerd" + session.toString());
	}

}
