package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class ShowTbCmd implements MySQLCommand {

	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();

	public static final ShowTbCmd INSTANCE = new ShowTbCmd();

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

		//TODO
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
