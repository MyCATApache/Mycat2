package io.mycat.proxy.man.cmds;

import io.mycat.mycat2.ConfigLoader;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.*;
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
        if (ProxyRuntime.INSTANCE.getMyCLuster().getClusterState() != MyCluster.ClusterState.Clustered) {
            LOGGER.warn("node is not clustered state, cluster may crashed, received older pkg, throw it");
            return;
        }

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

    public boolean sendPreparePacket(ConfigEnum configEnum, Object bean, String attach) {
        MycatConfig config = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        MyCluster cluster = ProxyRuntime.INSTANCE.getMyCLuster();
        byte type = configEnum.getType();

        if (cluster.configConfirmMap.get(type) != null) {
            LOGGER.warn("config is in config updating ...");
            return false;
        }

        // 获取新版本
        int nextRepIndexVersion = config.getNextConfigVersion(type);
        // 生成yml文件内容
        String content = YamlUtil.dump(bean);
        // 存储在本地prepare文件夹下
        YamlUtil.dumpToFile(YamlUtil.getFileName(configEnum.getFileName(), nextRepIndexVersion), content);
        // 构造prepare报文
        final ConfigPreparePacket preparePacket = new ConfigPreparePacket(type, nextRepIndexVersion, attach, content);
        // 向从节点发送报文
        cluster.configConfirmMap.put(type, new ConfigConfirmBean(0, nextRepIndexVersion));
        configAnswerAllAliveNodes(preparePacket, type, true);
        return true;
    }

    private void handlePrepare(AdminSession session) throws IOException {
        ConfigPreparePacket preparePacket = new ConfigPreparePacket();
        preparePacket.resolve(session.readingBuffer);
        byte configType = preparePacket.getConfType();
        int version = preparePacket.getConfVersion();

        ConfigEnum configEnum = ConfigEnum.getConfigEnum(configType);
        YamlUtil.dumpToFile(YamlUtil.getFileName(configEnum.getFileName(), version), preparePacket.getConfContent());

        // 从节点处理完成之后向主节点发送确认报文
        ConfigConfirmPacket confirmPacket = new ConfigConfirmPacket(configType, version, preparePacket.getAttach());
        session.answerClientNow(confirmPacket);

        ProxyRuntime runtime = ProxyRuntime.INSTANCE;
        runtime.addDelayedJob(() -> {
            MyCluster cluster = runtime.getMyCLuster();
            ConfigConfirmBean confirmBean = cluster.configConfirmMap.get(configType);
            if (confirmBean == null || confirmBean.confirmVersion != version) {
                LOGGER.debug("config update for version: {} is over, no need to check", version);
                return;
            }
            if (confirmBean.confirmCount != 0) {
                // prepare报文超过指定时间，集群没有全部响应
                LOGGER.error("cluster not send confirm packet in time for config type: {}, version: {}", configType, version);
                //todo config update 命令处理需要给前端返回
            }
            cluster.configConfirmMap.remove(configType);
        }, runtime.getProxyConfig().getPrepareDelaySeconds());
    }

    private void handleConfirm(AdminSession session) throws IOException {
        ConfigConfirmPacket confirmPacket = new ConfigConfirmPacket();
        confirmPacket.resolve(session.readingBuffer);
        byte configType = confirmPacket.getConfType();
        int version = confirmPacket.getConfVersion();

        MyCluster cluster = ProxyRuntime.INSTANCE.getMyCLuster();
        ConfigConfirmBean confirmBean = cluster.configConfirmMap.get(configType);
        if (confirmBean == null) {
            LOGGER.warn("may overtime to confirm for config type: {}, version: {}", configType, version);
            return;
        }
        if (version != confirmBean.confirmVersion) {
            LOGGER.warn("received packet contains old version {}, current version {}", version, confirmBean.confirmVersion);
            return;
        }

        if (--confirmBean.confirmCount == 0) {
            cluster.configConfirmMap.remove(configType);
            // 收到所有从节点的响应后给从节点发送确认报文
            String attach = confirmPacket.getAttach();
            ConfigCommitPacket commitPacket = new ConfigCommitPacket(configType, version, attach);
            configAnswerAllAliveNodes(commitPacket, configType, false);

            ConfigEnum configEnum = ConfigEnum.getConfigEnum(configType);
            ConfigLoader.INSTANCE.load(configEnum, commitPacket.getConfVersion());

            if (configEnum == ConfigEnum.REPLICA_INDEX) {
                MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
                ProxyRuntime.INSTANCE.startSwitchDataSource(attach, conf.getRepIndex(attach));
            }
        }
    }

    private void handleCommit(AdminSession session) throws IOException {
        ConfigCommitPacket commitPacket = new ConfigCommitPacket();
        commitPacket.resolve(session.readingBuffer);
        byte configType = commitPacket.getConfType();

        ConfigEnum configEnum = ConfigEnum.getConfigEnum(configType);
        ConfigLoader.INSTANCE.load(configEnum, commitPacket.getConfVersion());

        if (configEnum == ConfigEnum.REPLICA_INDEX) {
            MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
            String attach = commitPacket.getAttach();
            ProxyRuntime.INSTANCE.startSwitchDataSource(attach, conf.getRepIndex(attach));
        }
    }

    private void configAnswerAllAliveNodes(ManagePacket packet, byte type, boolean needCommit) {
        MyCluster cluster = ProxyRuntime.INSTANCE.getMyCLuster();
        ProxyRuntime.INSTANCE.getAdminSessionManager().getAllSessions().forEach(adminSession -> {
            AdminSession nodeSession = (AdminSession) adminSession;
            if (nodeSession.isChannelOpen()) {
                try {
                    nodeSession.answerClientNow(packet);
                    if (needCommit) {
                        ConfigConfirmBean confirmBean = cluster.configConfirmMap.get(type);
                        if (confirmBean != null) {
                            confirmBean.confirmCount++;
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("notify node err " + nodeSession.getNodeId(), e);
                }
            }
        });
    }
}
