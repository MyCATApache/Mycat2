package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * Desc: 节点向主节点发送配置请求报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigReqPacket extends ManagePacket {
    private int confCount;
    private byte[] confTypes;

    public ConfigReqPacket() {
        super(ManagePacket.PKG_CONFIG_REQ);
    }

    @Override
    public void resolveBody(ProxyBuffer buffer) {
        confCount = (int) buffer.readFixInt(4);
        confTypes = new byte[confCount];
        for (int i = 0; i < confCount; i++) {
            confTypes[i] = buffer.readByte();
        }
    }

    @Override
    public void writeBody(ProxyBuffer buffer) {
        buffer.writeFixInt(4, confCount);
        for (int i = 0; i < confCount; i++) {
            buffer.writeByte(confTypes[i]);
        }
    }

    public int getConfCount() {
        return confCount;
    }

    public void setConfCount(int confCount) {
        this.confCount = confCount;
    }

    public byte[] getConfTypes() {
        return confTypes;
    }

    public void setConfTypes(byte[] confTypes) {
        this.confTypes = confTypes;
    }
}
