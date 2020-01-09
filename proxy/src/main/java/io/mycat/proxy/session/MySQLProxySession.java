package io.mycat.proxy.session;

import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.ProxyBuffer;
import io.mycat.proxy.packet.MySQLPacketResolver;

import java.io.IOException;

/**
 * @author jamie12221
 *  date 2019-05-07 23:54 mycat session与mysql session 作为代理交换数据的handler
 **/
public interface MySQLProxySession<T extends Session<T>> extends Session<T> {

  ProxyBuffer currentProxyBuffer();


  void setCurrentProxyBuffer(ProxyBuffer buffer);

  MySQLPacketResolver getBackendPacketResolver();

  /**
   * 读取通道的数据,该方法在mycat 与mysql session都作为通道读
   */
  default boolean readFromChannel() throws IOException {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
//    proxyBuffer.newBufferIfNeed();
    proxyBuffer.compactInChannelReadingIfNeed();
    boolean b = proxyBuffer.readFromChannel(this.channel());
    updateLastActiveTime();
    return b;
  }


  /**
   * 读取完整的payload
   */
  default boolean readProxyPayloadFully() {
    return getBackendPacketResolver().readMySQLPayloadFully();
  }

  /**
   * 获取当前的payload,此时下标就是payload的开始位置 使用该方法后需要调用resetCurrentProxyPayload释放资源
   */
  default MySQLPacket currentProxyPayload() {
    return getBackendPacketResolver().currentPayload();
  }

  /**
   * 释放payload资源
   */
  default void resetCurrentProxyPayload() {
    getBackendPacketResolver().resetPayload();
  }

  /**
   * 尽可能地读取payload,可能获得的payload并不完整
   */
  default boolean readPartProxyPayload() throws IOException {
    return getBackendPacketResolver().readMySQLPacket();
  }

  /**
   * 释放buffer相关资源,但是在mycat session中并不清除proxybuffer对象,而在mysql session清除proxybuffer
   */
  default void resetPacket() {
    getBackendPacketResolver().reset();
  }

  long getSelectLimit();
}
