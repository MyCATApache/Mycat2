package io.mycat.proxy.man.cmds;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.ProxyStarter;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ManagePacket;
import io.mycat.proxy.man.MyCluster;
import io.mycat.proxy.man.packet.ConfigReqPacket;
import io.mycat.proxy.man.packet.ConfigResPacket;
import io.mycat.proxy.man.packet.ConfigVersionResPacket;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Desc: 用来处理配置相关报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigPacketCommand implements AdminCommand {
    public static final ConfigPacketCommand INSTANCE = new ConfigPacketCommand();
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPacketCommand.class);

    @Override
    public void handlerPkg(AdminSession session, byte cmdType) throws IOException {
        if (ProxyRuntime.INSTANCE.getMyCLuster().getClusterState() != MyCluster.ClusterState.Clustered) {
            LOGGER.warn("node is not clustered state, cluster may crashed, received older pkg, throw it");
            return;
        }

        if (cmdType == ManagePacket.PKG_CONFIG_VERSION_REQ) {
            handleConfigVersionReq(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_VERSION_RES) {
            handleConfigVersionRes(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_REQ) {
            handleConfigReq(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_RES) {
            handleConfigRes(session);
        } else {
            LOGGER.warn("Maybe Bug, Leader us want you to fix it ");
        }
    }

    /**
     * 主节点处理从节点发送来的配置文件版本报文
     * @param session
     * @throws IOException
     */
    private void handleConfigVersionReq(AdminSession session) throws IOException {
        LOGGER.debug("receive config version request package from {}", session.getNodeId());
        MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
        Map<ConfigEnum, Integer> configVersionMap = conf.getConfigVersionMap();
        ConfigVersionResPacket versionResPacket = new ConfigVersionResPacket(configVersionMap.size());
        int i = 0;
        for (Map.Entry<ConfigEnum, Integer> entry : configVersionMap.entrySet()) {
            versionResPacket.getConfTypes()[i] = entry.getKey().getType();
            versionResPacket.getConfVersions()[i] = entry.getValue();
            i++;
        }
        LOGGER.debug("send  version response package to {}", session.getNodeId());
        session.answerClientNow(versionResPacket);
    }

    /**
     * 从节点处理主节点发送来的配置文件版本报文
     * @param session
     * @throws IOException
     */
    private void handleConfigVersionRes(AdminSession session) throws IOException {
        LOGGER.debug("receive config version response package from {}", session.getNodeId());
        ConfigVersionResPacket respPacket = new ConfigVersionResPacket();
        respPacket.resolve(session.readingBuffer);
        int confCount = respPacket.getConfCount();
        byte[] confTypes = respPacket.getConfTypes();
        int[] confVersions = respPacket.getConfVersions();
        MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
        List<Byte> confTypeList = new ArrayList<>();
        for (int i = 0; i < confCount; i++) {
            ConfigEnum configEnum = ConfigEnum.getConfigEnum(confTypes[i]);
            if (conf.getConfigVersion(configEnum) != confVersions[i]) {
                // 配置文件版本不相同，需要加载
                confTypeList.add(confTypes[i]);
            }
        }
        if (confTypeList.isEmpty()) {
            LOGGER.debug("config version is same as leader, no need to load");
            ProxyStarter.INSTANCE.startProxy(false);
            return;
        }
        int count = confTypeList.size();
        byte[] types = new byte[count];
        for (int i = 0; i < count; i++) {
            types[i] = confTypeList.get(i);
        }
        ConfigReqPacket reqPacket = new ConfigReqPacket();
        reqPacket.setConfCount(count);
        reqPacket.setConfTypes(types);
        LOGGER.debug("send  config request package to {}", session.getNodeId());
        session.answerClientNow(reqPacket);
        session.confCount = count;
    }

    /**
     * 主节点处理从节点的配置获取报文
     * @param session
     * @throws IOException
     */
    private void handleConfigReq(AdminSession session) throws IOException {
        LOGGER.debug("receive config request packet from {}", session.getNodeId());
        MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
        ConfigReqPacket reqPacket = new ConfigReqPacket();
        reqPacket.resolve(session.readingBuffer);
        int count = reqPacket.getConfCount();
        byte[] types = reqPacket.getConfTypes();
        for (int i = 0; i < count; i++) {
            byte type = types[i];
            ConfigEnum configEnum = ConfigEnum.getConfigEnum(type);
            if (configEnum == null) {
                LOGGER.warn("config type is error: {}", type);
                continue;
            }
            int confVersion = conf.getConfigVersion(configEnum);
            String confMsg = YamlUtil.dump(conf.getConfig(configEnum));
            ConfigResPacket resPacket = new ConfigResPacket(configEnum.getType(), confVersion, confMsg);
            session.answerClientNow(resPacket);
        }
    }

    /**
     * 从节点处理主节点发送的配置报文
     * @param session
     * @throws IOException
     */
    private void handleConfigRes(AdminSession session) throws IOException {
        LOGGER.debug("receive config response packet from {}", session.getNodeId());
        ConfigResPacket resPacket = new ConfigResPacket();
        resPacket.resolve(session.readingBuffer);

        ConfigEnum configEnum = ConfigEnum.getConfigEnum(resPacket.getConfType());
        if (configEnum == null) {
            LOGGER.warn("config type is error: {}", resPacket.getConfType());
            return;
        }
        YamlUtil.dumpToFile(configEnum.getFileName() + "-" + resPacket.getConfVersion(), resPacket.getConfContent());
        if (--session.confCount == 0) {
            LOGGER.debug("receive config from leader over, start to load");
            ProxyStarter.INSTANCE.startProxy(false);
        }
    }
}
