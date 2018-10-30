package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.beans.conf.TableDefBean;
import io.mycat.mysql.Fields;
import io.mycat.mysql.packet.*;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ErrorCode;
import io.mycat.util.PacketUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class ShowTbCmd implements MySQLCommand {
	
	protected static Logger logger = LoggerFactory.getLogger(ShowTbCmd.class);

	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();

	public static final ShowTbCmd INSTANCE = new ShowTbCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {

		// 获取show tables from schemal语句中的schemal
		String showSchemal = session.sqlContext.getTableName(0);
		SchemaBean schema = null;
		if (showSchemal != null) {
			schema = ProxyRuntime.INSTANCE.getConfig().getSchemaBean(showSchemal);
			if (schema == null) {
				ErrorPacket error = new ErrorPacket();
				error.errno = ErrorCode.ER_BAD_DB_ERROR;
				error.packetId = 1;
				error.message = "Unknown database '" + showSchemal + "'";

				session.responseOKOrError(error);
				return false;
			}
		}
        schema = (schema != null ? schema : session.mycatSchema);

		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Tables in " + schema.getName(), Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;

		ProxyBuffer buffer = session.proxyBuffer;
		buffer.reset();

		// write header
		header.write(buffer);

		// write fields
		for (FieldPacket field : fields) {
			field.write(buffer);
		}
		// write eof
		eof.write(buffer);

		// write rows
		packetId = eof.packetId;

		for (TableDefBean table : schema.getTables()) {

			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(table.getName().getBytes());
			row.packetId = ++packetId;
			row.write(buffer);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		lastEof.write(buffer);

		buffer.flip();
		buffer.readIndex = buffer.writeIndex;
		session.writeToChannel();
		return false;
	}

	@Override
    public boolean onBackendResponse(MySQLSession session) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {

		session.proxyBuffer.flip();
		session.takeOwner(SelectionKey.OP_READ);
	
		logger.info("****************sessionID:"+session.getSessionId()+"isRead:"+session.getProxyBuffer().isInReading());
		return true;
	}

	@Override
    public boolean onBackendWriteFinished(MySQLSession session) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		if (sessionCLosed) {
			session.recycleAllocedBuffer(session.getProxyBuffer());
			session.unbindAllBackend();
		}
	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub

	}

}
