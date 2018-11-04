package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ErrorCode;
import io.mycat.util.ParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

        if (schemaBean == null && SchemaBean.SchemaTypeEnum.DB_IN_ONE_SERVER != session.mycatSchema.getSchemaType()) {
            ErrorPacket error = new ErrorPacket();
            error.errno = ErrorCode.ER_BAD_DB_ERROR;
            error.packetId = session.proxyBuffer.getByte(session.curMSQLPackgInf.startPos 
					+ ParseUtil.mysql_packetHeader_length);
            error.message = "Unknown database '" + schema + "'";
            session.responseOKOrError(error);
            return false;
		}else if(schemaBean!=null){
            session.mycatSchema = schemaBean;
            session.responseOKOrError(OKPacket.OK);
            return false;
        } else if (SchemaBean.SchemaTypeEnum.DB_IN_ONE_SERVER == session.mycatSchema.getSchemaType()) {
            DNBean defaultDN = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap()
                    .get(session.mycatSchema.getDefaultDataNode());
            defaultDN.setDatabase(schema);
			return super.procssSQL(session);
		}else{
			logger.warn("Unknown database '" + schema + "'");
		}		
		return false;
	}
}
