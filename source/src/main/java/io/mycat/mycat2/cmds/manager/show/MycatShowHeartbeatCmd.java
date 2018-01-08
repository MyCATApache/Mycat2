package io.mycat.mycat2.cmds.manager.show;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.LinkedList;
import java.util.List;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.heartbeat.MySQLDetector;
import io.mycat.mycat2.beans.heartbeat.MySQLHeartbeat;
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

public class MycatShowHeartbeatCmd implements MySQLCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatShowHelpCmd.class);
    public static final MycatShowHeartbeatCmd INSTANCE = new MycatShowHeartbeatCmd();

    private static final int FIELD_COUNT = 11;
    //private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("RS_CODE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("RETRY", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TIMEOUT", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("EXECUTE_TIME",Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("LAST_ACTIVE_TIME",Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("STOP", Fields.FIELD_TYPE_VAR_STRING);
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
		for (RowDataPacket row : getRows()) {
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
    
	private  List<RowDataPacket> getRows() {
		//DateFormat format = new SimpleDateFormat(DATE_FORMAT);
		List<RowDataPacket> list = new LinkedList<RowDataPacket>();
		
        ProxyRuntime.INSTANCE.getConfig().getMysqlRepMap().forEach((repName, repBean) -> {
            repBean.getMetaBeans().forEach(metaBean -> {
            	RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            	row.add(metaBean.getDsMetaBean().getHostName().getBytes());
            	row.add(metaBean.isSlaveNode()? "Slave".getBytes() : "Master".getBytes());
            	row.add(metaBean.getDsMetaBean().getIp().getBytes());
            	row.add(Integer.toString(metaBean.getDsMetaBean().getPort()).getBytes());
            	row.add(Integer.toString(metaBean.getHeartbeat().getStatus()).getBytes());
            	row.add(Integer.toString(metaBean.getHeartbeat().getErrorCount()).getBytes());
            	row.add(metaBean.getHeartbeat().isChecking() ? "checking".getBytes() : "idle".getBytes());
            	row.add(Long.toString(metaBean.getHeartbeat().getTimeout()).getBytes());            	
            	MySQLDetector detector=((MySQLHeartbeat) metaBean.getHeartbeat()).getDetector();
            	row.add(String.valueOf(detector.getLasstReveivedQryTime()-detector.getLastSendQryTime()).getBytes());
            	row.add(metaBean.getHeartbeat().getLastActiveTime().getBytes());            	
            	row.add(metaBean.getHeartbeat().isStop() ? "true".getBytes() : "false".getBytes());
            	list.add(row);
            });
        });		
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
