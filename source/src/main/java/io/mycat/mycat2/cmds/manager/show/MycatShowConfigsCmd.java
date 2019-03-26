package io.mycat.mycat2.cmds.manager.show;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import io.mycat.mysql.packet.ColumnDefPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.Fields;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.Configurable;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.PacketUtil;
import io.mycat.util.YamlUtil;

/**
 * mycat管理命令处理类，用于查询当前配置信息
 *
 * @date: 12/10/2017
 * @author: gaozhiwen
 */
public class MycatShowConfigsCmd implements MySQLCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatShowConfigsCmd.class);
    public static final MycatShowConfigsCmd INSTANCE = new MycatShowConfigsCmd();

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final ColumnDefPacket[] fields = new ColumnDefPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("name", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("version", Fields.FIELD_TYPE_INT24);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("lastUpdateTime", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("content", Fields.FIELD_TYPE_VAR_STRING);
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
        for (ColumnDefPacket field : fields) {
            field.write(buffer);
        }

        // write eof
        eof.write(buffer);

        // write rows
        byte packetId = eof.packetId;
		MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();

		DateFormat format = new SimpleDateFormat(DATE_FORMAT);
		for (ConfigEnum configEnum : ConfigEnum.values()) {
			Configurable confValue = conf.getConfig(configEnum);
			if (confValue == null) {
				continue;
			}
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(configEnum.name().getBytes());
			row.add(Integer.toString(conf.getConfigVersion(configEnum)).getBytes());
			row.add(format.format(conf.getConfigUpdateTime(configEnum)).getBytes());
			row.add(YamlUtil.dump(confValue).getBytes());
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
        return true;
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
    public void clearResouces(MycatSession session, boolean sessionCLosed) {
        // TODO Auto-generated method stub

    }

}
