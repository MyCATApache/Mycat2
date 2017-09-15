package io.mycat.proxy.man.cmds;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ManagePacket;
import io.mycat.proxy.man.MyCluster;
import io.mycat.proxy.man.packet.ConfigPreparePacket;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Desc: 用来处理配置更新相关报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigUpdatePacketCommand implements AdminCommand {
    public static final ConfigUpdatePacketCommand INSTANCE = new ConfigUpdatePacketCommand();
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUpdatePacketCommand.class);

    private ConfigUpdatePacketCommand() {}

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

    public void sendPreparePacket(String repName, int newIndex) throws IOException {
        ConfigEnum configEnum = ConfigEnum.REPLICA_INDEX;
        byte type = configEnum.getType();

        // 更新replica-index信息
        MycatConfig config = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        ReplicaIndexBean bean = new ReplicaIndexBean();
        Map<String, Integer> map = new HashMap<>(config.getRepIndexMap());
        map.put(repName, newIndex);
        bean.setReplicaIndexes(map);

        // 获取新版本
        int nextRepIndexVersion = config.getNextConfigVersion(type);

        // 生成yml文件内容
        String content = YamlUtil.dump(bean);

        // 存储在本地prepare文件夹下
        YamlUtil.dumpToFile(configEnum.getFileName() + "-" + nextRepIndexVersion, content);

        // 构造prepare报文
        final ConfigPreparePacket preparePacket = new ConfigPreparePacket();
        preparePacket.setConfType(type);
        preparePacket.setConfVersion(config.getNextConfigVersion(type));
        preparePacket.setConfContent(content);

        // 向从节点发送报文
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
