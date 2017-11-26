package io.mycat.mycat2.cmds.manager.show;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.LinkedList;
import java.util.List;
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
import io.mycat.mycat2.common.NameableExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MycatShowThreadPool implements MySQLCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatShowHelpCmd.class);
    public static final MycatShowThreadPool INSTANCE = new MycatShowThreadPool();

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("POOL_SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TASK_QUEUE_SIZE",Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("COMPLETED_TASK",Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TOTAL_TASK",Fields.FIELD_TYPE_LONGLONG);
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
		List<NameableExecutor> executors = getExecutors();
		for (NameableExecutor exec : executors) {
			if (exec != null) {
				RowDataPacket row = getRow(exec, session.charSet.charset);
				row.packetId = ++packetId;
				row.write(buffer);
			}
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
    
	private  RowDataPacket getRow(NameableExecutor exec, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(exec.getName().getBytes());
		row.add(Long.toString(exec.getPoolSize()).getBytes());
		row.add(Long.toString(exec.getActiveCount()).getBytes());
		row.add(Long.toString(exec.getQueue().size()).getBytes());
		row.add(Long.toString(exec.getCompletedTaskCount()).getBytes());
		row.add(Long.toString(exec.getTaskCount()).getBytes());
		return row;
	}

	private  List<NameableExecutor> getExecutors() {
		List<NameableExecutor> list = new LinkedList<NameableExecutor>();
		ProxyRuntime server = ProxyRuntime.INSTANCE;
		list.add(server.getTimerExecutor());
		list.add(server.getBusinessExecutor());
		return list;
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
        session.proxyBuffer.flip();
        // session.chnageBothReadOpts();
        session.takeOwner(SelectionKey.OP_READ);
        return false;
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub

    }
}
