package io.mycat.proxy.session;

import io.mycat.MycatExpection;
import io.mycat.proxy.MySQLPacketExchanger;
import io.mycat.proxy.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
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

  static void writeProxyBufferToChannel(MySQLProxySession proxySession) throws IOException {
    proxySession.currentProxyBuffer().writeToChannel(proxySession.channel());
    proxySession.updateLastActiveTime();
    proxySession.checkWriteFinished();
  }

  static void checkWriteFinished(MySQLProxySession proxySession) throws IOException {
    ProxyBuffer proxyBuffer = proxySession.currentProxyBuffer();
    if (!proxyBuffer.channelWriteFinished()) {
      proxySession.change2WriteOpts();
    } else {
      proxySession.writeFinished(proxySession);
    }
  }

  void setCurrentProxyBuffer(ProxyBuffer buffer);

  MySQLPacketResolver getPacketResolver();

  void switchMySQLProxy();

  default void rebuildProxyRequest(byte[] bytes) {
    ProxyBuffer proxyBuffer = this.currentProxyBuffer();
    proxyBuffer.reset();
    proxyBuffer.newBuffer(bytes);
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

  default boolean readFromChannel() throws IOException {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    proxyBuffer.compactInChannelReadingIfNeed();
    boolean b = proxyBuffer.readFromChannel(this.channel());
    updateLastActiveTime();
    return b;
  }

  default void writeProxyBufferToChannel(byte[] bytes) throws IOException {
    switchMySQLProxy();
    writeProxyBufferToChannel(this, bytes);
  }

  default void writeToChannel() throws IOException {
    writeProxyBufferToChannel(this);
  }

  default void checkWriteFinished() throws IOException {
    checkWriteFinished(this);
  }

  default MySQLPacket newCurrentProxyPacket(int packetLength) {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    proxyBuffer.reset();
    proxyBuffer.newBuffer(packetLength);
    MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
    mySQLPacket.writeSkipInWriting(4);
    return mySQLPacket;
  }

  default void writeProxyPacket(MySQLPacket packet) throws IOException {
    int packetId = getPacketResolver().getAndIncrementPacketId();
    writeProxyPacket(packet, packetId);
  }

  default void writeProxyPacket(MySQLPacket ogrin, int packetId) throws IOException {
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

  default void writeProxyBufferToChannel(ProxyBuffer proxyBuffer) throws IOException {
    switchMySQLProxy();
    this.setCurrentProxyBuffer(proxyBuffer);
    this.writeToChannel();
  }

  default boolean readProxyPayloadFully() throws IOException {
    return getPacketResolver().readMySQLPayloadFully();
  }

  default MySQLPacket currentProxyPayload() throws IOException {
    return getPacketResolver().currentPayload();
  }

  default void resetCurrentProxyPayload() throws IOException {
    getPacketResolver().resetPayload();
  }

  default boolean readPartProxyPayload() throws IOException {
    return getPacketResolver().readMySQLPacket();
  }

  default void resetPacket() {
    getPacketResolver().reset();
  }


  enum WriteHandler implements MycatSessionWriteHandler {
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
            MySQLPacketExchanger.INSTANCE.onFrontWriteFinished(proxySession);
          } else {
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
