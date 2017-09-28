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

    public void sendNotifyCmd(LeaderNotifyPacket pkg, AdminSession adminSession) {
        if (adminSession.isChannelOpen()) {
            try {
                adminSession.answerClientNow(pkg);
            } catch (Exception e) {
                LOGGER.warn("notify node err " + adminSession.getNodeId(), e);
            }
        }
    }

    public void sendNotifyCmd(LeaderNotifyPacket pkg) {
        ProxyRuntime.INSTANCE.getAdminSessionManager().getAllSessions()
                .forEach(adminSession -> sendNotifyCmd(pkg, adminSession));
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
        switch (packet.getType()) {
            case LeaderNotifyPacket.LOAD_CHARACTER:
                String repName = packet.getDetail();
                int index = Integer.valueOf(packet.getAttach());
                MySQLRepBean repBean = ProxyRuntime.INSTANCE.getConfig().getMySQLRepBean(repName);
                if (repBean == null) {
                    LOGGER.warn("leader packet repName may error, replica name: {}", repName);
                    return;
                }
                MySQLMetaBean metaBean = repBean.getMetaBeans().get(index);
                if (metaBean == null) {
                    LOGGER.warn("leader packet index may error, index: {}", index);
                    return;
                }
                metaBean.init();
                metaBean.getHeartbeat().setStatus(DBHeartbeat.OK_STATUS);
                break;
            default:
                LOGGER.warn("Maybe Bug, Leader us want you to fix it ");
        }
    }
}
