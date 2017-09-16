package io.mycat.proxy.man.cmds;

import io.mycat.mycat2.ConfigLoader;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ManagePacket;
import io.mycat.proxy.man.MyCluster;
import io.mycat.proxy.man.packet.ConfigCommitPacket;
import io.mycat.proxy.man.packet.ConfigConfirmPacket;
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

    public void sendPreparePacket(ConfigEnum configEnum, Object bean) {
        MycatConfig config = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        byte type = configEnum.getType();

        // 获取新版本
        int nextRepIndexVersion = config.getNextConfigVersion(type);

        // 生成yml文件内容
        String content = YamlUtil.dump(bean);

        // 存储在本地prepare文件夹下
        YamlUtil.dumpToFile(YamlUtil.getFileName(configEnum.getFileName(), nextRepIndexVersion), content);

        // 构造prepare报文
        final ConfigPreparePacket preparePacket = new ConfigPreparePacket();
        preparePacket.setConfType(type);
        preparePacket.setConfVersion(config.getNextConfigVersion(type));
        preparePacket.setConfContent(content);

        // 向从节点发送报文
        MyCluster cluster = ProxyRuntime.INSTANCE.getMyCLuster();
        cluster.needCommitVersion = nextRepIndexVersion;
        cluster.needCommitCount = 0;
        configAnswerAllAliveNodes(preparePacket, true);
    }

    private void handlePrepare(AdminSession session) throws IOException {
        ConfigPreparePacket preparePacket = new ConfigPreparePacket();
        preparePacket.resolveBody(session.readingBuffer);
        byte configType = preparePacket.getConfType();
        int version = preparePacket.getConfVersion();

        ConfigEnum configEnum = ConfigEnum.getConfigEnum(configType);

        YamlUtil.dumpToFile(YamlUtil.getFileName(configEnum.getFileName(), version), preparePacket.getConfContent());

        // 从节点处理完成之后向主节点发送确认报文
        ConfigConfirmPacket confirmPacket = new ConfigConfirmPacket(configType, version);
        session.answerClientNow(confirmPacket);

        ProxyRuntime runtime = ProxyRuntime.INSTANCE;
        runtime.addDelayedJob(() -> {
            MyCluster cluster = runtime.getMyCLuster();
            if (cluster.needCommitCount != 0) {
                // prepare报文超过指定时间，集群没有全部响应
                cluster.needCommitVersion = -1;
                //todo config update 命令处理需要给前端返回
            }
        }, runtime.getProxyConfig().getPrepareDelaySeconds());
    }

    private void handleConfirm(AdminSession session) throws IOException {
        ConfigConfirmPacket confirmPacket = new ConfigConfirmPacket();
        confirmPacket.resolveBody(session.readingBuffer);
        byte configType = confirmPacket.getConfType();
        int version = confirmPacket.getConfVersion();

        MyCluster cluster = ProxyRuntime.INSTANCE.getMyCLuster();
        if (version != cluster.needCommitVersion) {
            LOGGER.warn("received packet contains old version {}, current version {}", version, cluster.needCommitVersion);
            return;
        }

        ConfigEnum configEnum = ConfigEnum.getConfigEnum(configType);
        cluster.needCommitCount--;

        if (cluster.needCommitCount == 0) {
            // 收到所有从节点的响应后给从节点发送确认报文
            ConfigCommitPacket commitPacket = new ConfigCommitPacket(configType, version);
            configAnswerAllAliveNodes(commitPacket, false);

            ConfigLoader.INSTANCE.load(configEnum, commitPacket.getConfVersion());
            ProxyRuntime.INSTANCE.startHeartBeatScheduler();
        }
    }

    private void handleCommit(AdminSession session) throws IOException {
        ConfigCommitPacket commitPacket = new ConfigCommitPacket();
        commitPacket.resolveBody(session.readingBuffer);
        byte configType = commitPacket.getConfType();

        ConfigEnum configEnum = ConfigEnum.getConfigEnum(configType);

        ConfigLoader.INSTANCE.load(configEnum, commitPacket.getConfVersion());
    }

    private void configAnswerAllAliveNodes(ManagePacket packet, boolean needCommit) {
        MyCluster cluster = ProxyRuntime.INSTANCE.getMyCLuster();
        ProxyRuntime.INSTANCE.getAdminSessionManager().getAllSessions().forEach(adminSession -> {
            AdminSession nodeSession = (AdminSession) adminSession;
            if (nodeSession.isChannelOpen()) {
                try {
                    nodeSession.answerClientNow(packet);
                    if (needCommit) {
                        cluster.needCommitCount++;
                    }
                } catch (Exception e) {
                    LOGGER.warn("notify node err " + nodeSession.getNodeId(),e);
                }
            }
        });
    }
}
