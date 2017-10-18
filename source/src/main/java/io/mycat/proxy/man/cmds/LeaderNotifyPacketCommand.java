package io.mycat.proxy.man.cmds;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.heartbeat.DBHeartbeat;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.*;
import io.mycat.proxy.man.packet.LeaderNotifyPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Desc: 用来处理配置更新相关报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class LeaderNotifyPacketCommand implements AdminCommand {
    public static final LeaderNotifyPacketCommand INSTANCE = new LeaderNotifyPacketCommand();
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderNotifyPacketCommand.class);

    public void sendNotifyCmd(byte type, String detail, String attach, AdminSession adminSession) {
        LeaderNotifyPacket pkg = new LeaderNotifyPacket(type, detail, attach);
        sendCmd(pkg, adminSession);
    }

    public void sendNotifyCmd(byte type, String detail, String attach) {
        ProxyRuntime.INSTANCE.getAdminSessionManager().getAllSessions().forEach(adminSession -> {
                LeaderNotifyPacket packet = new LeaderNotifyPacket(type, detail, attach);
                sendCmd(packet, adminSession);
            });
    }

    private void sendCmd(LeaderNotifyPacket packet, AdminSession adminSession) {
        if (adminSession.isChannelOpen()) {
            try {
                adminSession.answerClientNow(packet);
            } catch (Exception e) {
                LOGGER.warn("notify node err " + adminSession.getNodeId(), e);
            }
        }
    }

    @Override
    public void handlerPkg(AdminSession session, byte cmdType) throws IOException {
        if (ProxyRuntime.INSTANCE.getMyCLuster().getClusterState() != MyCluster.ClusterState.Clustered) {
            LOGGER.warn("node is not clustered state, cluster may crashed, received older pkg, throw it");
            return;
        }

        if (cmdType == ManagePacket.PKG_LEADER_NOTIFY) {
            handleCmd(session);
        } else {
            LOGGER.warn("Maybe Bug, Leader us want you to fix it ");
        }
    }

    /**
     * 处理leader发送来的命令报文
     *
     * @param session
     */
    private void handleCmd(AdminSession session) throws IOException {
        LeaderNotifyPacket packet = new LeaderNotifyPacket();
        packet.resolve(session.readingBuffer);

        byte type = packet.getType();
        if (type == LeaderNotifyPacket.LOAD_CHARACTER) {
            MySQLMetaBean metaBean = parsePacket(packet);
            if (metaBean != null) {
                loadCharacter(metaBean);
            }
        } else if (type == LeaderNotifyPacket.SLAVE_NODE_HEARTBEAT_ERROR) {
            MySQLMetaBean metaBean = parsePacket(packet);
            if (metaBean != null) {
                slaveNodeHeartbeatError(metaBean);
            }
        } else if (type == LeaderNotifyPacket.SLAVE_NODE_HEARTBEAT_SUCCESS) {
            MySQLMetaBean metaBean = parsePacket(packet);
            if (metaBean != null) {
                slaveNodeHeartbeatSuccess(metaBean);
            }
        } else {
            LOGGER.warn("Maybe Bug, Leader us want you to fix it ");
        }
    }

    private MySQLMetaBean parsePacket(LeaderNotifyPacket packet) {
        String repName = packet.getDetail();
        int index = Integer.valueOf(packet.getAttach());
        MySQLRepBean repBean = ProxyRuntime.INSTANCE.getConfig().getMySQLRepBean(repName);
        if (repBean == null) {
            LOGGER.warn("leader packet repName may error, replica name: {}", repName);
            return null;
        }

        MySQLMetaBean metaBean = repBean.getMetaBeans().get(index);
        if (metaBean == null) {
            LOGGER.warn("leader packet index may error, index: {}", index);
            return null;
        }
        return metaBean;
    }

    private void loadCharacter(MySQLMetaBean metaBean) throws IOException {
        metaBean.init();
        LOGGER.info("leader packet to set slave node ok, metaBean: {}", metaBean);
        metaBean.getHeartbeat().setStatus(DBHeartbeat.OK_STATUS);
    }

    private void slaveNodeHeartbeatError(MySQLMetaBean metaBean) {
        LOGGER.error("leader packet to set slave node error, metaBean: {}", metaBean);
        metaBean.getHeartbeat().setStatus(DBHeartbeat.ERROR_STATUS);
    }

    private void slaveNodeHeartbeatSuccess(MySQLMetaBean metaBean) {
        LOGGER.info("leader packet to set slave node success, metaBean: {}", metaBean);
        metaBean.getHeartbeat().setStatus(DBHeartbeat.OK_STATUS);
    }
}
