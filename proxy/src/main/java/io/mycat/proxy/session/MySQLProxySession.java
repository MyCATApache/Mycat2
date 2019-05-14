package io.mycat.proxy.session;

import io.mycat.proxy.NetMonitor;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.handler.MySQLPacketExchanger;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-07 23:54 mycat session与mysql session 作为代理交换数据的handler
 **/
public interface MySQLProxySession<T extends Session<T>> extends Session<T> {

  ProxyBuffer currentProxyBuffer();


  void setCurrentProxyBuffer(ProxyBuffer buffer);

  MySQLPacketResolver getPacketResolver();

  /**
   * 读取通道的数据,该方法在mycat 与mysql session都作为通道读
   */
  default boolean readFromChannel() throws IOException {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    proxyBuffer.compactInChannelReadingIfNeed();
    boolean b = proxyBuffer.readFromChannel(this.channel());
    updateLastActiveTime();
    return b;
  }


  /**
   * 读取完整的payload
   */
  default boolean readProxyPayloadFully() {
    return getPacketResolver().readMySQLPayloadFully();
  }

  /**
   * 获取当前的payload,此时下标就是payload的开始位置 使用该方法后需要调用resetCurrentProxyPayload释放资源
   */
  default MySQLPacket currentProxyPayload() {
    return getPacketResolver().currentPayload();
  }

  /**
   * 释放payload资源
   */
  default void resetCurrentProxyPayload() {
    getPacketResolver().resetPayload();
  }

  /**
   * 尽可能地读取payload,可能获得的payload并不完整
   */
  default boolean readPartProxyPayload() throws IOException {
    return getPacketResolver().readMySQLPacket();
  }

  /**
   * 释放buffer相关资源,但是在mycat session中并不清除proxybuffer对象,而在mysql session清除proxybuffer
   */
  default void resetPacket() {
    getPacketResolver().reset();
  }

  /**
   * 使用bytes构造Proxybuffer,此时Proxybuffer处于可读可写状态
   */
  default void resetProxyBuffer(byte[] bytes) {
    ProxyBuffer proxyBuffer = this.currentProxyBuffer();
    proxyBuffer.reset();
    proxyBuffer.newBuffer(bytes);
  }

  /**
   * 代理模式前端写入处理器
   */
  enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession mycat) throws IOException {
      ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
      int oldIndex = proxyBuffer.channelWriteStartIndex();
      proxyBuffer.writeToChannel(mycat.channel());

      NetMonitor.onFrontWrite(mycat, proxyBuffer.currentByteBuffer(), oldIndex,
          proxyBuffer.channelReadEndIndex());
      mycat.updateLastActiveTime();

      if (!proxyBuffer.channelWriteFinished()) {
        mycat.change2WriteOpts();
      } else {
        MySQLClientSession mysql = mycat.currentBackend();
        if (mysql == null) {
          assert false;
        } else {
          boolean b = MySQLPacketExchanger.INSTANCE.onFrontWriteFinished(mycat);
          if (b) {
            mycat.onHandlerFinishedClear(true);
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
