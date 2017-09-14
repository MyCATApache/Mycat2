package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * Desc: 节点加入集群后向主节点发送配置文件版本的相应报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class ConfigVersionResPacket extends ManagePacket {
    private int confCount;
    private byte[] confTypes;
    private int[] confVersions;

    public ConfigVersionResPacket() {
        super(ManagePacket.PKG_CONFIG_VERSION_RES);
    }

    public ConfigVersionResPacket(int confCount) {
        super(ManagePacket.PKG_CONFIG_VERSION_RES);
        this.confCount = confCount;
        this.confTypes = new byte[confCount];
        this.confVersions = new int[confCount];
    }

    @Override
    public void resolveBody(ProxyBuffer buffer) {
        confCount = (int) buffer.readFixInt(4);
        confTypes = new byte[confCount];
        confVersions = new int[confCount];
        for (int i = 0; i < confCount; i++) {
            confTypes[i] = buffer.readByte();
            confVersions[i] = (int) buffer.readFixInt(4);
        }
    }

    @Override
    public void writeBody(ProxyBuffer buffer) {
        buffer.writeFixInt(4, confCount);
        for (int i = 0; i < confCount; i++) {
            buffer.writeByte(confTypes[i]);
            buffer.writeFixInt(4, confVersions[i]);
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

    public int[] getConfVersions() {
        return confVersions;
    }

    public void setConfVersions(int[] confVersions) {
        this.confVersions = confVersions;
    }
}
