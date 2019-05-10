package io.mycat.proxy.session;

import io.mycat.MycatExpection;
import io.mycat.proxy.MycatSessionWriteHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.MySQLProxyHandler;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author jamie12221
 * @date 2019-05-07 23:54
 **/
public interface MySQLProxySession<T extends Session<T>> extends Session<T> {

  ProxyBuffer currentProxyBuffer();

  public MySQLPacketResolver getPacketResolver();
 default void switchMySQLProxyWriteHandler(){

 }
  void setCurrentProxyBuffer(ProxyBuffer buffer);
  default void rebuildProxyRequest(byte[] bytes){
      ProxyBuffer proxyBuffer = this.currentProxyBuffer();
      proxyBuffer.reset();
      proxyBuffer.newBuffer(bytes);
  }
  public default boolean readFromChannel() throws IOException {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    proxyBuffer.compactInChannelReadingIfNeed();
    boolean b = proxyBuffer.readFromChannel(this.channel());
    updateLastActiveTime();
    return b;
  }

  public default void writeProxyBufferToChannel(byte[] bytes) throws IOException {
    switchMySQLProxyWriteHandler();
    writeProxyBufferToChannel(this, bytes);
  }

  static void writeProxyBufferToChannel(MySQLProxySession proxySession, byte[] bytes)
      throws IOException {
    ProxyBuffer buffer = proxySession.currentProxyBuffer();
    buffer.reset();
    buffer.newBuffer(bytes);
    buffer.channelWriteStartIndex(0);
    buffer.channelWriteEndIndex(bytes.length);
    proxySession.writeToChannel();
  }

  public default void writeToChannel() throws IOException {
    writeProxyBufferToChannel(this);
  }

  public static void writeProxyBufferToChannel(MySQLProxySession proxySession) throws IOException {
    proxySession.currentProxyBuffer().writeToChannel(proxySession.channel());
    proxySession.updateLastActiveTime();
    proxySession.checkWriteFinished();
  }

  public default void checkWriteFinished() throws IOException {
    checkWriteFinished(this);
  }

  public static void checkWriteFinished(MySQLProxySession proxySession) throws IOException {
    ProxyBuffer proxyBuffer = proxySession.currentProxyBuffer();
    if (!proxyBuffer.channelWriteFinished()) {
      proxySession.change2WriteOpts();
    } else {
      proxySession.writeFinished(proxySession);
    }
  }

  public default MySQLPacket newCurrentProxyPacket(int packetLength) {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    proxyBuffer.reset();
    proxyBuffer.newBuffer(packetLength);
    MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
    mySQLPacket.writeSkipInWriting(4);
    return mySQLPacket;
  }

  public default void writeProxyPacket(MySQLPacket packet) throws IOException {
    int packetId = getPacketResolver().getAndIncrementPacketId();
    writeProxyPacket(packet, packetId);
  }

  public default void writeProxyPacket(MySQLPacket ogrin, int packetId) throws IOException {
    switchMySQLProxyWriteHandler();
    ProxyBufferImpl mySQLPacket1 = (ProxyBufferImpl) ogrin;
    ByteBuffer buffer = mySQLPacket1.currentByteBuffer();
    int packetEndPos = buffer.position();
    int payloadLen = buffer.position() - 4;
    if (payloadLen < 0xffffff) {
      ogrin.putFixInt(0, 3, payloadLen);
      ogrin.putByte(3, (byte) packetId);
      ProxyBuffer packet1 = (ProxyBuffer) ogrin;
      packet1.channelWriteStartIndex(0);
      packet1.channelWriteEndIndex(packetEndPos);
      writeToChannel();
    } else {
      throw new MycatExpection("unsupport!");
    }
  }

  public default void writeProxyBufferToChannel(ProxyBuffer proxyBuffer) throws IOException {
    switchMySQLProxyWriteHandler();
    this.setCurrentProxyBuffer(proxyBuffer);
    this.writeToChannel();
  }

  public default boolean readProxyPayloadFully() throws IOException {
    return getPacketResolver().readMySQLPayloadFully();
  }

  public default MySQLPacket currentProxyPayload() throws IOException {
    return getPacketResolver().currentPayload();
  }

  public default void resetCurrentProxyPayload() throws IOException {
    getPacketResolver().resetPayload();
  }

  public default boolean readPartProxyPayload() throws IOException {
    return getPacketResolver().readMySQLPacket();
  }

  public default void resetPacket() {
    getPacketResolver().reset();
  }


  public static enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession proxySession) throws IOException {
      proxySession.currentProxyBuffer().writeToChannel(proxySession.channel());
      proxySession.updateLastActiveTime();

      ProxyBuffer proxyBuffer = proxySession.currentProxyBuffer();
      if (!proxyBuffer.channelWriteFinished()) {
        proxySession.change2WriteOpts();
      } else {
        if (proxySession.isResponseFinished()) {
          proxySession.resetPacket();
          proxySession.change2ReadOpts();
        } else {
          MySQLClientSession backend = proxySession.getBackend();
          if (backend != null) {
            MySQLProxyHandler.INSTANCE.onFrontWriteFinished(proxySession);
          }else {
            onWriteFinished(proxySession);
          }
        }
      }
    }

    @Override
    public void onWriteFinished(MycatSession proxySession) throws IOException {
      proxySession.writeFinished(proxySession);
    }
  }

}
