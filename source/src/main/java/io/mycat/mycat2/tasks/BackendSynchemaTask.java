package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mysql.packet.CommandPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ErrorCode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Map;

public class BackendSynchemaTask extends AbstractBackendIOTask<MySQLSession> {
	
	private static Logger logger = LoggerFactory.getLogger(BackendSynchemaTask.class);
	
	public BackendSynchemaTask(MySQLSession session) throws IOException{
		super(session,true);
        String databases = findDatabase(session);
		logger.debug("the Backend Synchema Task begin ");
		logger.debug(" use  "+databases);
		session.proxyBuffer.reset();
		CommandPacket packet = new CommandPacket();
		packet.packetId = 0;
		packet.command = MySQLCommand.COM_INIT_DB;
		packet.arg = databases.getBytes();
		packet.write(session.proxyBuffer);
		session.proxyBuffer.flip();
		session.proxyBuffer.readIndex = session.proxyBuffer.writeIndex;
		try {
			session.writeToChannel();
		}catch(ClosedChannelException e){
			if(session.getMycatSession()!=null){
				session.close(false, "backend connection is closed!");
			}
			session.close(false, e.getMessage());
			return;
		}  catch (Exception e) {
			logger.debug("the Backend Synchema Task end ");
            String errmsg = "backend sync mycatSchema task Error. " + e.getMessage();
			errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
			errPkg.message = errmsg;
			logger.error(errmsg);
			e.printStackTrace();
			this.finished(false);
		}
	}

	@Override
	public void onSocketRead(MySQLSession session) throws IOException {
		session.proxyBuffer.reset();
		
		try {
            if (!session.readFromChannel()) {
                return;
            }
		}catch(ClosedChannelException e){
			session.close(false, e.getMessage());
			return;
		}catch (IOException e) {
			logger.debug("the Backend Synchema Task end ");
            String errmsg = "the backend sync mycatSchema task Error." + e.getMessage();
			errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno = ErrorCode.ER_UNKNOWN_ERROR;
			errPkg.message = errmsg;
			logger.error(errmsg);
			e.printStackTrace();
			this.finished(false);
			return;
		}
		
        switch (session.resolveMySQLPackage(true)) {
            case Full:
                if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
                    String database = findDatabase(session);
                    session.setDatabase(database);
                    logger.debug("the Backend Synchema Task end ");
                    this.finished(true);
                } else if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {
                    errPkg = new ErrorPacket();
                    errPkg.read(session.proxyBuffer);
                    logger.debug("the Backend Synchema Task end ");
                    logger.warn("backend state sync Error.Err No. " + errPkg.errno + ","
                            + errPkg.message);
                    this.finished(false);
                }
                break;
            default:
                return;
        }
	}
	
	private String findDatabase(MySQLSession session) {
	    String replicaName =
                session.getMySQLMetaBean().getRepBean().getReplicaBean().getName();
        Map<String, DNBean> dataNodeMap = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap();
        String databases = "";
        if (dataNodeMap != null) {
            DNBean dataNode = dataNodeMap.values().stream().filter(dn -> {
                return dn.getReplica().equalsIgnoreCase(replicaName);
            }).findFirst().orElse(null);
            if (dataNode != null) {
                databases = dataNode.getDatabase();
            }
        }
        if (StringUtils.isEmpty(databases)) {
            databases = dataNodeMap.get(session.getMycatSession().mycatSchema.getDefaultDataNode())
                    .getDatabase();
        }
        return databases;
	}

}
