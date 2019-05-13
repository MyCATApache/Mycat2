package io.mycat.proxy.session;

import io.mycat.proxy.MySQLPacketExchanger;
import io.mycat.proxy.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-07 23:54
 **/
public interface MySQLProxySession<T extends Session<T>> extends Session<T> {

  ProxyBuffer currentProxyBuffer();


  void setCurrentProxyBuffer(ProxyBuffer buffer);

  MySQLPacketResolver getPacketResolver();


  default boolean readFromChannel() throws IOException {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    proxyBuffer.compactInChannelReadingIfNeed();
    boolean b = proxyBuffer.readFromChannel(this.channel());
    updateLastActiveTime();
    return b;
  }


  default boolean readProxyPayloadFully() {
    return getPacketResolver().readMySQLPayloadFully();
  }

  default MySQLPacket currentProxyPayload() {
    return getPacketResolver().currentPayload();
  }

  default void resetCurrentProxyPayload() {
    getPacketResolver().resetPayload();
  }

  default boolean readPartProxyPayload() throws IOException {
    return getPacketResolver().readMySQLPacket();
  }

  default void resetPacket() {
    getPacketResolver().reset();
  }


  default void resetProxyBuffer(byte[] bytes) {
    ProxyBuffer proxyBuffer = this.currentProxyBuffer();
    proxyBuffer.reset();
    proxyBuffer.newBuffer(bytes);
  }

  enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession mycat) throws IOException {
      mycat.currentProxyBuffer().writeToChannel(mycat.channel());
      mycat.updateLastActiveTime();

      ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
      if (!proxyBuffer.channelWriteFinished()) {
        mycat.change2WriteOpts();
      } else {
        if (mycat.isResponseFinished()) {
          mycat.change2ReadOpts();
          mycat.onHandlerFinishedClear(true);
        } else {
          MySQLClientSession mysql = mycat.currentBackend();
          if (mysql != null) {
            boolean b = MySQLPacketExchanger.INSTANCE.onFrontWriteFinished(mycat);
            if (b) {
              mycat.onHandlerFinishedClear(true);
            }
          } else {
            assert false;//mycat session使用了Proxybuffer写入
          }
        }
      }
    }

    /**
     * mycat seesion没有重写onWriteFinished方法,所以onWriteFinished调用的是此类的writeToChannel方法
     */
    @Override
    public void onWriteFinished(MycatSession proxySession) throws IOException {
      proxySession.writeFinished(proxySession);
    }
  }
}
