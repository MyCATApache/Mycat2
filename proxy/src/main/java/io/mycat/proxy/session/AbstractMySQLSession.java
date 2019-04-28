package io.mycat.proxy.session;

import io.mycat.beans.MySQLMeta;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolverImpl;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.payload.MySQLPayloadReadView;
import io.mycat.proxy.task.AsynTaskCallBack;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public abstract class AbstractMySQLSession<T extends AbstractSession> extends AbstractSession<T> {
    public AbstractMySQLSession(Selector selector, SocketChannel channel, int socketOpt, NIOHandler nioHandler, SessionManager<? extends Session> sessionManager) throws IOException {
        super(selector, channel, socketOpt, nioHandler, sessionManager);
    }

    private String charset;
    private String clientUser;
    private MySQLAutoCommit autoCommit;
    private MySQLIsolation isolation = MySQLIsolation.REPEATED_READ;


    private AsynTaskCallBack<T> callBack;

    final MySQLPayloadReadView mySQLPayloadReadView = new MySQLPayloadReadView();

    public MySQLPayloadReadView getPayloadReader() {
        mySQLPayloadReadView.init(getPacketResolver());
        return mySQLPayloadReadView;
    }

    public byte setPacketId(int packetId) {
        return (byte) this.packetResolver.setPacketId(packetId);
    }

    public byte getPacketId() {
        return (byte) this.packetResolver.getPacketId();
    }

    public byte incrementPacketIdAndGet() {
        return (byte) this.packetResolver.incrementPacketIdAndGet();
    }

    public AsynTaskCallBack<T> getCallBackAndReset() {
        AsynTaskCallBack<T> callBack = this.callBack;
        this.callBack = null;
        return callBack;
    }

    public void setCallBack(AsynTaskCallBack<T> callBack) {
        this.callBack = callBack;
    }


    public void setCharset(String charset) {
        this.charset = charset;
    }

    public void setClientUser(String clientUser) {
        this.clientUser = clientUser;
    }

    public void setAutoCommit(MySQLAutoCommit autoCommit) {
        this.autoCommit = autoCommit;
    }

    public MySQLIsolation getIsolation() {
        return isolation;
    }

    public void setIsolation(MySQLIsolation isolation) {
        this.isolation = isolation;
    }

    protected final MySQLPacketResolver packetResolver = new MySQLPacketResolverImpl(false, MySQLMeta.getClientCapabilityFlags(), this);

    public MySQLPacketResolver getPacketResolver() {
        return packetResolver;
    }

    public MySQLPacket currentFullPayload() {
        int startPos = packetResolver.getStartPos();
        int endPos = packetResolver.getEndPos();
        MySQLPacket mySQLPacket = packetResolver.currentProxybuffer();
        mySQLPacket.packetReadStartIndex(startPos + 3);
        int i = mySQLPacket.readByte() & 0xff;
        mySQLPacket.packetReadEndIndex(endPos);
        return mySQLPacket;
    }


    public String getCharset() {
        return charset;
    }

    public String getClientUser() {
        return clientUser;
    }

    public MySQLAutoCommit getAutoCommit() {
        return autoCommit;
    }


    public MySQLPacket newCurrentMySQLPacket() {
        ProxyBuffer proxyBuffer = currentProxyBuffer();
        proxyBuffer.reset();
        proxyBuffer.newBuffer();
        MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
        mySQLPacket.writeSkipInWriting(4);
        return mySQLPacket;
    }

    public MySQLPacket newCurrentMySQLPacket(int packetLength) {
        ProxyBuffer proxyBuffer = currentProxyBuffer();
        proxyBuffer.reset();
        proxyBuffer.newBuffer(packetLength);
        MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
        mySQLPacket.writeSkipInWriting(4);
        return mySQLPacket;
    }

    public void writeMySQLPacket(MySQLPacket packet) throws IOException {
        int packetId = packetResolver.getAndIncrementPacketId();
        writeMySQLPacket(packet, packetId);
    }

    public void writeMySQLPacket(MySQLPacket packet, int packetId) throws IOException {
        int i = packet.packetWriteIndex();
        packet.putFixInt(0, 3, i - 4);
        packet.putByte(3, (byte) packetId);
        ProxyBuffer packet1 = (ProxyBuffer) packet;
        packet1.channelWriteStartIndex(0);
        packet1.channelWriteEndIndex(i);
        writeToChannel();
    }
    public boolean readMySQLPayloadFully() throws IOException {
        return packetResolver.readMySQLPayloadFullyNotProxy();
    }

    public boolean readMySQLPacketFully() throws IOException {
        return packetResolver.readMySQLPacketFully();
    }

    public boolean readMySQLPacket() throws IOException {
        return packetResolver.readMySQLPacket();
    }

    public void setResponseFinished(boolean b) {
        packetResolver.setRequestFininshed(b);
    }

    public boolean isResponseFinished() {
        return packetResolver.isResponseFinished();
    }

    public boolean isRequestFinished() {
        return packetResolver.isRequestFininshed();
    }

    public void setRequestFinished(boolean requestFinished) {
        this.packetResolver.setRequestFininshed(requestFinished);
    }

    public void resetPacket() {
        packetResolver.reset();
    }

    public MySQLPayloadType getPayloadType() {
        return this.packetResolver.getPailoadType();
    }

    public boolean isActivated() {
        long timeInterval = System.currentTimeMillis() - this.lastActiveTime;
        return (timeInterval < 60 * 1000);//60 second
    }


}
