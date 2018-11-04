package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.Fields;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.FieldPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.PacketUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Set;

public class ShowDbCmd implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(ShowDbCmd.class);

	public static final ShowDbCmd INSTANCE = new ShowDbCmd();

	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();

	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {

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
		byte packetId = eof.packetId;

		Set<String> schemaSet = ProxyRuntime.INSTANCE.getConfig().getMycatSchemaMap().keySet();
		for (String schema : schemaSet) {

			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(schema.getBytes());
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
		return false;
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
