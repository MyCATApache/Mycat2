package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * Desc: 主节点向从节点发送通知的报文
 *
 * @date: 11/09/2017
 * @author: gaozhiwen
 */
public class LeaderNotifyPacket extends ManagePacket {
    public static final byte LOAD_CHARACTER = 1;

    private byte type;
    private String detail;
    private String attach;

    public LeaderNotifyPacket() {
        super(ManagePacket.PKG_LEADER_NOTIFY);
    }

    public LeaderNotifyPacket(byte type, String detail, String attach) {
        super(ManagePacket.PKG_LEADER_NOTIFY);
        this.type = type;
        this.detail = detail;
        this.attach = attach;
    }

    @Override
    public void resolveBody(ProxyBuffer buffer) {
        this.type = buffer.readByte();
        this.detail = buffer.readNULString();
        this.attach = buffer.readNULString();
    }

    @Override
    public void writeBody(ProxyBuffer buffer) {
        buffer.writeByte(type);
        buffer.writeNULString(detail);
        buffer.writeNULString(attach);
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public String getAttach() {
        return attach;
    }

    public void setAttach(String attach) {
        this.attach = attach;
    }
}
