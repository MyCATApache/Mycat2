package io.mycat.mycat2.cmds.manager.show;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.MycatSessionManager;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

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

        fields[i] = PacketUtil.getField("Id", Fields.FIELD_TYPE_INT24);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("User", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;  
        
        fields[i] = PacketUtil.getField("Host", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("db", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;  
        
       // fields[i] = PacketUtil.getField("charSet", Fields.FIELD_TYPE_VAR_STRING);
       // fields[i++].packetId = ++packetId;       
       
        fields[i] = PacketUtil.getField("Time", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("Info", Fields.FIELD_TYPE_VAR_STRING);
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
			if (mycatSession.clientUser==null){
				row.add(String.valueOf("null").getBytes());	
			}
			else {
			  row.add(mycatSession.clientUser.getBytes());
			}
			row.add(mycatSession.addr.getBytes());
            row.add(mycatSession.mycatSchema.name.getBytes());
			//row.add(mycatSession.charSet.charset.getBytes());
			row.add(Long.toString(System.currentTimeMillis()-mycatSession.startTime).getBytes());
		//	row.add(mycatSession.sqlContext.getRealSQL(mycatSession.sqlContext.getSQLCount()-1).getBytes());		
			row.add(mycatSession.sqlContext.getRealSQL(0).getBytes());		
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
        // session.chnageBothReadOpts();
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
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub

    }
}