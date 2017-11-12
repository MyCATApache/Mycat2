package io.mycat.mycat2.cmds.manager;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.MycatSessionManager;
import io.mycat.mycat2.cmds.MycatCmds;
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
import io.mycat.util.StringUtil;
import io.mycat.util.YamlUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Map.Entry;

/**
 * mycat管理命令处理类，用于查询当前Session信息
 *
 * @date: 7/11/2017
 * @author: wind
 */
public class MycatShowHelpCmd implements MySQLCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatShowHelpCmd.class);
    public static final MycatShowHelpCmd INSTANCE = new MycatShowHelpCmd();

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("STATEMENT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;  
        
        fields[i] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
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
        
		for (Entry<String, String> entry : MycatCmds.INSTANCE.getCmdDescMap().entrySet()) {  
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);			
			String cmd="mycat show "+entry.getKey();
			row.add(cmd.getBytes());
			row.add(entry.getValue().getBytes("gbk"));		
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
