package io.mycat.mycat2.cmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyRuntime;

public class ComInitDB extends DirectPassthrouhCmd{
	
	
	private static final Logger logger = LoggerFactory.getLogger(ComInitDB.class);

	public static final ComInitDB INSTANCE = new ComInitDB();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
//		byte[] curdatabases = session.proxyBuffer.getBytes(session.curMSQLPackgInf.startPos+MySQLPacket.packetHeaderSize+1,
//				session.curMSQLPackgInf.pkgLength -MySQLPacket.packetHeaderSize+1);
//		if(curdatabases==null){
//			
//		}
//		
//		if(ProxyRuntime.INSTANCE.getProxyConfig() instanceof MycatConfig){
//			MycatConfig config = (MycatConfig)ProxyRuntime.INSTANCE.getProxyConfig();
//			String databasesName = new String(curdatabases);
//			session.schema = config.getMycatSchema(databasesName);
//			logger.debug(" current database change to {}",databasesName);
//		}
		
		return super.procssSQL(session);
	}
}
