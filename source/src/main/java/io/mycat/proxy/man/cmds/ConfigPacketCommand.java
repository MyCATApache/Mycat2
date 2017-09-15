package io.mycat.proxy.man.cmds;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.ProxyStarter;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ManagePacket;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPacketCommand.class);

    @Override
    public void handlerPkg(AdminSession session, byte cmdType) throws IOException {
        if (cmdType == ManagePacket.PKG_CONFIG_VERSION_REQ) {
            // 主节点处理从节点发送来的配置文件版本报文
            handleConfigVersionReq(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_VERSION_RES) {
            // 从节点处理主节点发送来的配置文件版本报文
            handleConfigVersionRes(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_REQ) {
            // 主节点处理从节点的配置获取报文
            handleConfigReq(session);
        } else if (cmdType == ManagePacket.PKG_CONFIG_RES) {
            // 从节点处理主节点发送的配置报文
            handleConfigRes(session);
        } else {
            LOGGER.warn("Maybe Bug, Leader us want you to fix it ");
        }
    }

    private void handleConfigVersionReq(AdminSession session) throws IOException {
        LOGGER.debug("receive config version request package from {}", session.getNodeId());
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        Map<Byte, Integer> configVersionMap = conf.getConfigVersionMap();
        ConfigVersionResPacket versionResPacket =
                new ConfigVersionResPacket(configVersionMap.size());
        int i = 0;
        for (Map.Entry<Byte, Integer> entry : configVersionMap.entrySet()) {
            versionResPacket.getConfTypes()[i] = entry.getKey();
            versionResPacket.getConfVersions()[i] = entry.getValue();
            i++;
        }
        session.answerClientNow(versionResPacket);
    }

    private void handleConfigVersionRes(AdminSession session) throws IOException {
        LOGGER.debug("receive config version response package from {}", session.getNodeId());
        ConfigVersionResPacket respPacket = new ConfigVersionResPacket();
        respPacket.resolve(session.readingBuffer);
        int confCount = respPacket.getConfCount();
        byte[] confTypes = respPacket.getConfTypes();
        int[] confVersions = respPacket.getConfVersions();
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        List<Byte> confTypeList = new ArrayList<>();
        for (int i = 0; i < confCount; i++) {
            if (conf.getConfigVersion(confTypes[i]) != confVersions[i]) {
                // 配置文件版本不相同，需要加载
                confTypeList.add(confTypes[i]);
            }
        }
        if (confTypeList.isEmpty()) {
            LOGGER.debug("config version is same as leader, no need to load");
            ProxyStarter.INSTANCE.startProxy();
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
        session.answerClientNow(reqPacket);
        session.confCount = count;
    }

    private void handleConfigReq(AdminSession session) throws IOException {
        LOGGER.debug("receive config request packet from {}", session.getNodeId());
        ProxyConfig conf = ProxyRuntime.INSTANCE.getProxyConfig();
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
            byte confType = configEnum.getCode();
            int confVersion = conf.getConfigVersion(confType);
            String confMsg = YamlUtil.dump(conf.getConfig(confType));
            ConfigResPacket resPacket = new ConfigResPacket(confType, confVersion, confMsg);
            session.answerClientNow(resPacket);
        }
    }

    private void handleConfigRes(AdminSession session) throws IOException {
        LOGGER.debug("receive config response packet from {}", session.getNodeId());
        ConfigResPacket resPacket = new ConfigResPacket();
        resPacket.resolve(session.readingBuffer);

        ConfigEnum configEnum = ConfigEnum.getConfigEnum(resPacket.getConfType());
        if (configEnum == null) {
            LOGGER.warn("config type is error: {}", resPacket.getConfType());
            return;
        }
        YamlUtil.dumpToFile(configEnum.getFileName() + "-" + resPacket.getConfVersion(), resPacket.getConfMessage());
        if (--session.confCount == 0) {
            LOGGER.debug("receive config from leader over, start to load");
            ProxyStarter.INSTANCE.startProxy();
        }
    }
}
