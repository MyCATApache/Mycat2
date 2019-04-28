package io.mycat.proxy.handler;

import io.mycat.beans.Schema;
import io.mycat.beans.mysql.*;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.MysqlNativePasswordPluginUtil;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.packet.AuthPacketImpl;
import io.mycat.proxy.packet.HandshakePacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.MainMycatNIOHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MySQLClientAuthHandler implements NIOHandler<MycatSession> {
    private static final Logger logger = LoggerFactory.getLogger(MySQLClientAuthHandler.class);
    public byte[] seed;
    public  MycatSession mycat;
    private boolean finished = false;
    private static final byte[] AUTH_OK = new byte[]{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};
    public void setMycatSession(MycatSession mycatSession) {
        this.mycat = mycatSession;
    }

    @Override
    public void onSocketRead(MycatSession mycat) throws IOException {
        mycat.currentProxyBuffer().newBufferIfNeed();
        if (mycat.getCurNIOHandler() != this){
            return;
        }
        if(!mycat.readFromChannel()){
            return;
        }
        if (!mycat.readMySQLPacketFully()) {
            return;
        }
        MySQLPacket mySQLPacket = mycat.currentFullPayload();
        AuthPacketImpl auth = new AuthPacketImpl();
        auth.readPayload(mySQLPacket);

        mycat.setAutoCommit(MySQLAutoCommit.OFF);
        Schema defaultSchema = MycatRuntime.INSTANCE.getMycatConfig().getDefaultSchema();
        mycat.setSchema(defaultSchema);
        mycat.setIsolation(MySQLIsolation.READ_UNCOMMITTED);
        MySQLCollationIndex collationIndex = defaultSchema.getDefaultDataNode().getReplica().getCollationIndex();
        String charset = collationIndex.getCharsetByIndex((int) auth.characterSet);
        mycat.setCharset(charset);
        finished = true;
        mycat.writeToChannel(AUTH_OK);
    }

    @Override
    public void onSocketWrite(MycatSession mysql) throws IOException {
        mysql.writeToChannel();
    }

    @Override
    public void onWriteFinished(MycatSession mycat) throws IOException {
        mycat.currentProxyBuffer().reset();
        if (!finished) {
            mycat.change2ReadOpts();
        }else {
            mycat.resetPacket();
            mycat.switchNioHandler(MainMycatNIOHandler.INSTANCE);
        }
    }

    @Override
    public void onSocketClosed(MycatSession session, boolean normal) {

    }

    public void sendAuthPackge() {
        try {
            byte[][] seedParts = MysqlNativePasswordPluginUtil.nextSeedBuild();
            this.seed = seedParts[2];
            HandshakePacketImpl hs = new HandshakePacketImpl();
            hs.protocolVersion = MySQLVersion.PROTOCOL_VERSION;
            hs.serverVersion = new String(MySQLVersion.SERVER_VERSION);
            hs.connectionId = mycat.sessionId();
            hs.authPluginDataPartOne = new String(seedParts[0]);
            hs.capabilities = new MySQLCapabilityFlags(mycat.getServerCapabilities());
            hs.hasPartTwo = true;
            hs.characterSet = 8;
            hs.statusFlags = 2;
            hs.authPluginDataLen = 21; // 有插件的话，总长度必是21, seed
            hs.authPluginDataPartTwo = new String(seedParts[1]);
            hs.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;
            MySQLPacket mySQLPacket = mycat.newCurrentMySQLPacket();
            hs.writePayload(mySQLPacket);
            mycat.writeMySQLPacket(mySQLPacket,0);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
