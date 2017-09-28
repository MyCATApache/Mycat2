package io.mycat.mycat2.cmds;

import java.io.IOException;

import io.mycat.mycat2.beans.conf.SchemaBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ErrorCode;
import io.mycat.util.ParseUtil;

public class ComInitDB extends DirectPassthrouhCmd{
	
	
	private static final Logger logger = LoggerFactory.getLogger(ComInitDB.class);

	public static final ComInitDB INSTANCE = new ComInitDB();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		
		BufferSQLParser parser = new BufferSQLParser();
		int offset = session.curMSQLPackgInf.startPos+MySQLPacket.packetHeaderSize+1;
		int len = session.curMSQLPackgInf.pkgLength - MySQLPacket.packetHeaderSize - 1;
		parser.parse(session.proxyBuffer.getBuffer(),offset ,len, session.sqlContext);
		
		String schema = session.sqlContext.getBuffer().getString(offset, len);
		
		MycatConfig config = ProxyRuntime.INSTANCE.getConfig();
		SchemaBean schemaBean = config.getSchemaBean(schema);
		
		if (schemaBean == null && SchemaBean.SchemaTypeEnum.DB_IN_ONE_SERVER != session.schema.getSchemaType()) {
            ErrorPacket error = new ErrorPacket();
            error.errno = ErrorCode.ER_BAD_DB_ERROR;
            error.packetId = session.proxyBuffer.getByte(session.curMSQLPackgInf.startPos 
					+ ParseUtil.mysql_packetHeader_length);
            error.message = "Unknown database '" + schema + "'";
            session.responseOKOrError(error);
            return false;
		}else if(schemaBean!=null){
			session.schema = schemaBean;
            session.responseOKOrError(OKPacket.OK);
            return false;
		}else if(SchemaBean.SchemaTypeEnum.DB_IN_ONE_SERVER==session.schema.getSchemaType()){
			session.schema.getDefaultDN().setDatabase(schema);
			return super.procssSQL(session);
		}else{
			logger.warn("Unknown database '" + schema + "'");
		}		
		return false;
	}
}
