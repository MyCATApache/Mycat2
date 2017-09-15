package io.mycat.proxy.man.cmds;

import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ManagePacket;
import io.mycat.proxy.man.MyCluster;
import io.mycat.proxy.man.packet.ConfigPreparePacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Desc: 用来处理配置更新相关报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigUpdatePacketCommand implements AdminCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUpdatePacketCommand.class);

    @Override
    public void handlerPkg(AdminSession session, byte cmdType) throws IOException {
        if (cmdType == ManagePacket.PKG_CONFIG_PREPARE) {
            handlePrepare(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_CONFIRM) {
            handleConfirm(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_COMMIT) {
            handleCommit(session);
        } else {
            LOGGER.warn("Maybe Bug, Leader us want you to fix it ");
        }
    }

    private void sendPreparePacket(String content) {
        byte type = ConfigEnum.REPLICA_INDEX.getType();
        final ConfigPreparePacket preparePacket = new ConfigPreparePacket();
        preparePacket.setConfType(type);
        preparePacket.setConfVersion(ProxyRuntime.INSTANCE.getProxyConfig().getNextConfigVersion(type));
        preparePacket.setConfContent(content);

        MyCluster cluster = ProxyRuntime.INSTANCE.getMyCLuster();
        ProxyRuntime.INSTANCE.getAdminSessionManager().getAllSessions().forEach(adminSession -> {
            AdminSession nodeSession = (AdminSession) adminSession;
            if (nodeSession.isChannelOpen()) {
                try {
                    nodeSession.answerClientNow(preparePacket);
                    cluster.needCommitCount++;
                } catch (Exception e) {
                    LOGGER.warn("notify node err " + nodeSession.getNodeId(),e);
                }
            }
        });
    }

    private void handlePrepare(AdminSession session) {
        ConfigPreparePacket packet = new ConfigPreparePacket();
        packet.resolveBody(session.readingBuffer);
    }

    private void handleConfirm(AdminSession session) {

    }

    private void handleCommit(AdminSession session) {

    }
}
