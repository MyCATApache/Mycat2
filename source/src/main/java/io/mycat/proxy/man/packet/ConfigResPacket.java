package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * Desc: 主节点向从节点发送配置详情信息
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigResPacket extends ManagePacket {
    private byte confType;
    private int confVersion;
    private String confMessage;

    public ConfigResPacket() {
        super(ManagePacket.PKG_CONFIG_RES);
    }

    public ConfigResPacket(byte confType, int confVersion, String confMessage) {
        super(ManagePacket.PKG_CONFIG_RES);
        this.confType = confType;
        this.confVersion = confVersion;
        this.confMessage = confMessage;
    }

    @Override
    public void resolveBody(ProxyBuffer buffer) {
        this.confType = buffer.readByte();
        this.confVersion = (int) buffer.readFixInt(4);
        this.confMessage = buffer.readNULString();
    }

    @Override
    public void writeBody(ProxyBuffer buffer) {
        buffer.writeByte(confType);
        buffer.writeFixInt(4, confVersion);
        buffer.writeNULString(confMessage);
    }

    public byte getConfType() {
        return confType;
    }

    public void setConfType(byte confType) {
        this.confType = confType;
    }

    public int getConfVersion() {
        return confVersion;
    }

    public void setConfVersion(int confVersion) {
        this.confVersion = confVersion;
    }

    public String getConfMessage() {
        return confMessage;
    }

    public void setConfMessage(String confMessage) {
        this.confMessage = confMessage;
    }
}
