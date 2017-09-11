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
    private int confCount;
    private byte[] confTypes;
    private String[] confMessages;

    public ConfigResPacket() {
        super(ManagePacket.PKG_CONFIG_RES);
    }

    @Override
    public void resolveBody(ProxyBuffer buffer) {
        confCount = (int) buffer.readFixInt(4);
        confTypes = new byte[confCount];
        confMessages = new String[confCount];
        for (int i = 0; i < confCount; i++) {
            confTypes[i] = buffer.readByte();
            confMessages[i] = buffer.readNULString();
        }
    }

    @Override
    public void writeBody(ProxyBuffer buffer) {
        buffer.writeFixInt(4, confCount);
        for (int i = 0; i < confCount; i++) {
            buffer.writeByte(confTypes[i]);
            buffer.writeNULString(confMessages[i]);
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

    public String[] getConfMessages() {
        return confMessages;
    }

    public void setConfMessages(String[] confMessages) {
        this.confMessages = confMessages;
    }
}
