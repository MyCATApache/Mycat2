package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * Desc: 节点加入集群后向主节点发送配置文件版本的请求报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigVersionReqPacket extends ManagePacket {
    public ConfigVersionReqPacket() {
        super(ManagePacket.PKG_CONFIG_VERSION_REQ);
    }

    @Override
    public void resolveBody(ProxyBuffer buffer) {

    }

    @Override
    public void writeBody(ProxyBuffer buffer) {

    }
}
