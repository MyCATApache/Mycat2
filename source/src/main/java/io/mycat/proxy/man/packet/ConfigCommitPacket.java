package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * Desc: 主节点向从节点发送commit报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigCommitPacket extends ManagePacket {
    private byte confType;
    private int confVersion;

    public ConfigCommitPacket() {
        super(ManagePacket.PKG_CONFIG_COMMIT);
    }

    public ConfigCommitPacket(byte confType, int confVersion) {
        super(ManagePacket.PKG_CONFIG_COMMIT);
        this.confType = confType;
        this.confVersion = confVersion;
    }

    @Override
    public void resolveBody(ProxyBuffer buffer) {
        this.confType = buffer.readByte();
        this.confVersion = (int) buffer.readFixInt(4);
    }

    @Override
    public void writeBody(ProxyBuffer buffer) {
        buffer.writeByte(confType);
        buffer.writeFixInt(4, confVersion);
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
}
