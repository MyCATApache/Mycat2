package io.mycat.mycat2.cmds.manager;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.MycatSessionManager;
import io.mycat.mysql.Fields;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.FieldPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.Configurable;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.SessionManager;
import io.mycat.util.PacketUtil;
import io.mycat.util.YamlUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Objects;

/**
 * mycat管理命令处理类，用于查询当前Session信息
 *
 * @date: 7/11/2017
 * @author: wind
 */
public class MycatShowSessionsCmd implements MySQLCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatShowSessionsCmd.class);
    public static final MycatShowSessionsCmd INSTANCE = new MycatShowSessionsCmd();

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("id", Fields.FIELD_TYPE_INT24);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("host", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("addr", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;   

        fields[i] = PacketUtil.getField("user", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;  
        
       // fields[i] = PacketUtil.getField("charSet", Fields.FIELD_TYPE_VAR_STRING);
       // fields[i++].packetId = ++packetId;  
        
        fields[i] = PacketUtil.getField("schema", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;  
        
        fields[i] = PacketUtil.getField("startTime", Fields.FIELD_TYPE_LONG);
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
        MycatSessionManager  SessionManager= (MycatSessionManager)ProxyRuntime.INSTANCE.getSessionManager();

        ArrayList<MycatSession> allSessions=(ArrayList<MycatSession>) SessionManager.getAllSessions();
        for(MycatSession mycatSession : allSessions)    {  
		    DateFormat format = new SimpleDateFormat(DATE_FORMAT);       	
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(Integer.toString(mycatSession.getSessionId()).getBytes());
			row.add(mycatSession.host.getBytes());
			row.add(mycatSession.addr.getBytes());
			if (mycatSession.clientUser==null){
				row.add(String.valueOf("null").getBytes());	
			}
			else {
			  row.add(mycatSession.clientUser.getBytes());
			}
			//row.add(mycatSession.charSet.charset.getBytes());			
			row.add(mycatSession.schema.name.getBytes());
			row.add(format.format(mycatSession.startTime).getBytes());
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
