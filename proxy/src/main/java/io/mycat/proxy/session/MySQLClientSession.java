/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session;

import io.mycat.MySQLAPI;
import io.mycat.MySQLSessionMonopolizeType;
import io.mycat.MycatExpection;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.logTip.TaskTip;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatMonitor;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.handler.MySQLPacketExchanger.MySQLIdleNIOHandler;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolverImpl;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.replica.MySQLDatasource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;

/**
 * @author jamie12221
 * @date 2019-05-07 21:47
 *
 * 后端MySQL Session
 **/
public class MySQLClientSession extends
    AbstractSession<MySQLClientSession> implements MySQLProxySession<MySQLClientSession>, MySQLAPI {

  protected AsyncTaskCallBack<MySQLClientSession> callBack;


  protected final MySQLPacketResolver packetResolver = new MySQLPacketResolverImpl(this);
  /**
   * mysql session的源配置信息
   */
  private final MySQLDatasource datasource;
  protected ProxyBuffer proxyBuffer;

  /**
   * 绑定的mycat 与同步的dataNode mycat的解绑 mycat = null即可
   */
  private MycatSession mycat;
  /**
   * //在发起请求的时候设置
   */
  private boolean noResponse = false;
  private boolean requestSuccess = false;
  private MycatDataNode dataNode;
  /**
   * 错误信息
   */
  private String lastMessage;
  private boolean isIdle;


  private String charset;

  private MySQLIsolation isolation;
  /**
   * 与mycat session绑定的信息 monopolizeType 是无法解绑的原因 TRANSACTION,事务 LOAD_DATA,交换过程
   * PREPARE_STATEMENT_EXECUTE,预处理过程 CURSOR_EXISTS 游标 以上四种情况 mysql客户端的并没有结束对mysql的交互,所以无法解绑
   */
  private MySQLSessionMonopolizeType monopolizeType = MySQLSessionMonopolizeType.NONE;


  public MycatSession getMycatSeesion() {
    return mycat;
  }

  public void setMycatSession(MycatSession mycat) {
    this.mycat = mycat;
  }


  /**
   * 构造函数
   */
  public MySQLClientSession(MySQLDatasource datasource, Selector selector, SocketChannel channel,
      int socketOpt,
      NIOHandler nioHandler, SessionManager<MySQLClientSession> sessionManager
  ) throws IOException {
    super(selector, channel, socketOpt, nioHandler, sessionManager);
    this.datasource = datasource;
    Objects.requireNonNull(datasource);
  }

  /**
   * 把bytes吸入通道
   */
  static void writeProxyBufferToChannel(MySQLProxySession proxySession, byte[] bytes)
      throws IOException {
    assert bytes != null;
    ProxyBuffer buffer = proxySession.currentProxyBuffer();
    buffer.reset();
    buffer.newBuffer(bytes);
    buffer.channelWriteStartIndex(0);
    buffer.channelWriteEndIndex(bytes.length);
    proxySession.writeToChannel();
  }

  /**
   * 0.本方法直接是关闭调用的第一个方法  1清理packetResolver(payload 2handler handler仅关闭handler自身的资源 ,packet) 3.与mycat
   * session1解除绑定
   */
  @Override
  public void close(boolean normal, String hint) {
    if (closed) {
      return;
    }
    resetPacket();
    closed = true;
    try {
      getSessionManager().removeSession(this, normal, hint);
    } catch (Exception e) {
      LOGGER.error(TaskTip.CLOSE_ERROR.getMessage(e));
    }
    NIOHandler curNIOHandler = getCurNIOHandler();
    if (curNIOHandler != null) {
      curNIOHandler.onSocketClosed(this, normal, hint);
    }
    switchNioHandler(null);
  }



  /**
   * 解除与mycat session的绑定 如果遇到事务等状态会解除失败 如果是isCLose = true则强制解除 本函数不实现关闭通道,如果强制解除绑定应该是close方法调用
   *
   * @param isClose 是否关闭事件
   */
//  public boolean unbindMycatIfNeed(boolean isClose) {
//    assert this.mycat != null;
//    setResponseFinished(false);
//    setNoResponse(false);
//    MycatSession mycat = this.mycat;
//    boolean monopolized = isMonopolized();
//    try {
//      if (monopolized && !isClose) {
//        return false;
//      }
//
//      this.resetPacket();
//      setCurrentProxyBuffer(null);
//
//      switchNioHandler(null);
//
//      this.mycat.setMySQLSession(null);
//      this.mycat.resetPacket();
//      this.mycat.switchWriteHandler(MySQLServerSession.WriteHandler.INSTANCE);
//      this.mycat = null;
//
//      if (!isClose) {
//        getSessionManager().addIdleSession(this);
//      }
//      return true;
//    } catch (Exception e) {
//      mycat.setLastMessage(e.getMessage());
//      mycat.writeErrorEndPacketBySyncInProcessError();
//      mycat.close(false, e.getMessage());
//    }
//    return true;
//  }


  public MycatDataNode getDataNode() {
    return dataNode;
  }

  public void setDataNode(MycatDataNode dataNode) {
    this.dataNode = dataNode;
  }

  public MySQLDatasource getDatasource() {
    return datasource;
  }

  public MycatSession getMycatSession() {
    return mycat;
  }

//  /**
//   * 执行透传时候设置
//   */
//  public void switchProxyNioHandler() {
//    assert this.mycat != null;
//    this.mycat.switchWriteHandler(WriteHandler.INSTANCE);
//    this.nioHandler = MySQLProxyNIOHandler.INSTANCE;
//  }

  /**
   * 准备接收响应时候
   */
  public void prepareReveiceResponse() {
    this.packetResolver.prepareReveiceResponse();
  }

  /**
   * 准备接收预处理PrepareOk时候
   */
  public void prepareReveicePrepareOkResponse() {
    this.packetResolver.prepareReveicePrepareOkResponse();
  }


  @Override
  public MySQLClientSession getThis() {
    return this;
  }

  /**
   * 计算mysql session是否被占用
   */
  public boolean isMonopolized() {
    MySQLSessionMonopolizeType monopolizeType = this.monopolizeType;
    if (MySQLSessionMonopolizeType.PREPARE_STATEMENT_EXECUTE == monopolizeType ||
            MySQLSessionMonopolizeType.LOAD_DATA == monopolizeType
    ) {
      return true;
    }
    int serverStatus = getPacketResolver().getServerStatus();
    if (
        MySQLServerStatusFlags
            .statusCheck(serverStatus, MySQLServerStatusFlags.IN_TRANSACTION)) {
      this.monopolizeType = (MySQLSessionMonopolizeType.TRANSACTION);
      return true;
    } else if (MySQLServerStatusFlags
                   .statusCheck(serverStatus, MySQLServerStatusFlags.CURSOR_EXISTS)) {
      this.monopolizeType = (MySQLSessionMonopolizeType.CURSOR_EXISTS);
      return true;
    } else {
      this.monopolizeType = (MySQLSessionMonopolizeType.NONE);
      return false;
    }
  }

  /**
   * 被预处理命令占用
   */
  public boolean isMonopolizedByPrepareStatement() {
    return this.monopolizeType == MySQLSessionMonopolizeType.PREPARE_STATEMENT_EXECUTE;
  }

  /**
   * 被loaddata占用
   */
  public boolean isMonopolizedByLoadData() {
    return this.monopolizeType == MySQLSessionMonopolizeType.LOAD_DATA;
  }

  /**
   * 设置占用类型
   */
  public void setMonopolizeType(MySQLSessionMonopolizeType monopolizeType) {
    this.monopolizeType = monopolizeType;
  }

  /**
   * 设置Proxybuffer,Proxybuffer只有两种来源 1.来自mycat session 2.来自task类
   */
  @Override
  public void setCurrentProxyBuffer(ProxyBuffer buffer) {
    this.proxyBuffer = buffer;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MySQLClientSession that = (MySQLClientSession) o;
    return sessionId == that.sessionId;
  }

  @Override
  public int hashCode() {
    int result = mycat != null ? mycat.hashCode() : 0;
    result = 31 * result + (datasource != null ? datasource.hashCode() : 0);
    result = 31 * result + (dataNode != null ? dataNode.hashCode() : 0);
    result = 31 * result + (monopolizeType != null ? monopolizeType.hashCode() : 0);
    return result;
  }

  /**
   * 强制设置packetId
   */
  public byte setPacketId(int packetId) {
    return (byte) this.packetResolver.setPacketId(packetId);
  }

  /**
   * 获取报文处理器的packetId
   */
  public byte getPacketId() {
    return (byte) this.packetResolver.getPacketId();
  }

  /**
   * ++PacketId
   */
  public byte incrementPacketIdAndGet() {
    return (byte) this.packetResolver.incrementPacketIdAndGet();
  }

  /**
   * 获取callback并把保存的callback设置为null
   */
  public AsyncTaskCallBack<MySQLClientSession> getCallBackAndReset() {
    AsyncTaskCallBack<MySQLClientSession> callBack = this.callBack;
    this.callBack = null;
    return callBack;
  }

  /**
   * 设置callback,一般是Task类设置
   */
  public void setCallBack(AsyncTaskCallBack<MySQLClientSession> callBack) {
    this.callBack = callBack;
  }

  /**
   * 获取错误的信息 可以为null
   */
  @Override
  public String getLastMessage() {
    return this.lastMessage;
  }

  /**
   * 设置错误信息
   */
  @Override
  public String setLastMessage(String lastMessage) {
    this.lastMessage = lastMessage;
    return lastMessage;
  }

  /**
   * 获取报文处理器
   */
  public MySQLPacketResolver getPacketResolver() {
    return packetResolver;
  }

  /**
   * 获取当前的网络缓冲区
   */
  public ProxyBuffer currentProxyBuffer() {
    return proxyBuffer;
  }

  /**
   * 根据报文处理器获取响应是否结束
   */
  public boolean isResponseFinished() {
    return packetResolver.isResponseFinished();
  }

  /**
   * 强制设置响应结束,一般在重置mysql session的时候设置
   */
  public void setResponseFinished(boolean b) {
    packetResolver.setRequestFininshed(b);
  }

  /**
   * 获取报文类型
   */
  public MySQLPayloadType getPayloadType() {
    return this.packetResolver.getMySQLPayloadType();
  }

  /**
   * 判断该session是否活跃
   */
  public boolean isActivated() {
    long timeInterval = currentTimeMillis() - this.lastActiveTime;
    return (timeInterval < 60 * 1000);//60 second
  }

  /**
   * 把buffer写入通道
   */
  public void writeToChannel() throws IOException {
    assert !isIdle();
    writeProxyBufferToChannel();
  }


  /**
   * 检测buffer是否写入完毕
   */
  public void checkWriteFinished() throws IOException {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    if (!proxyBuffer.channelWriteFinished()) {
      this.change2WriteOpts();
    } else {
      writeFinished(this);
    }
  }

  /**
   * 最终把buffer写入通道的方法
   */
  final void writeProxyBufferToChannel() throws IOException {
    ProxyBuffer proxyBuffer = this.currentProxyBuffer();
    int oldIndex = proxyBuffer.channelWriteStartIndex();
    proxyBuffer.writeToChannel(this.channel());
    MycatMonitor.onBackendWrite(this, proxyBuffer.currentByteBuffer(), oldIndex,
        proxyBuffer.channelWriteEndIndex());
    this.updateLastActiveTime();
    this.checkWriteFinished();
  }

  /**
   * 把当前的proxybuffer作为报文构造
   */
  public MySQLPacket newCurrentProxyPacket(int packetLength) {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    proxyBuffer.reset();
    proxyBuffer.newBuffer(packetLength);
    MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
    mySQLPacket.writeSkipInWriting(4);
    return mySQLPacket;
  }

  /**
   * 把bytes数据直接写入通道,不做修改
   */
  public void writeProxyBufferToChannel(byte[] bytes) throws IOException {
    writeProxyBufferToChannel(this, bytes);
  }

  /**
   * newCurrentProxyPacket的配套方法,把该方法构造的报文写入通道
   */
  public void writeCurrentProxyPacket(MySQLPacket ogrin, int packetId) throws IOException {
    ProxyBufferImpl mySQLPacket1 = (ProxyBufferImpl) ogrin;
    ByteBuffer buffer = mySQLPacket1.currentByteBuffer();
    int packetEndPos = buffer.position();
    int payloadLen = buffer.position() - 4;
    if (payloadLen < MySQLPacketSplitter.MAX_PACKET_SIZE) {
      ogrin.putFixInt(0, 3, payloadLen);
      ogrin.putByte(3, (byte) packetId);
      ProxyBuffer packet1 = (ProxyBuffer) ogrin;
      packet1.channelWriteStartIndex(0);
      packet1.channelWriteEndIndex(packetEndPos);
      writeToChannel();
    } else {
      throw new MycatExpection(TaskTip.UNSUPPORT_DEF_MAX_PACKET.getMessage(payloadLen));
    }
  }

  /**
   * 把proxybuffer的内容写入通道,proxybuffer需要保证处于写入状态 即channelWriteStartIndex == 0 channelWriteSEndIndex
   * 为写入结束位置
   */
  public void writeProxyBufferToChannel(ProxyBuffer proxyBuffer) throws IOException {
    assert proxyBuffer.channelWriteStartIndex() == 0;
    this.setCurrentProxyBuffer(proxyBuffer);
    this.writeToChannel();
  }

  /**
   * 清除buffer,每次命令开始之前和结束之后都要保证buffer是空的
   */
  public void resetPacket() {
    packetResolver.reset();
    setCurrentProxyBuffer(null);
  }

  public boolean isNoResponse() {
    return noResponse;
  }

  /**
   * 设置响应类型,有些报文是没有响应的
   */
  public void setNoResponse(boolean noResponse) {
    this.noResponse = noResponse;
  }

  /**
   * 切换出来处理器,在闲置状态中不能设置
   */
  @Override
  public void switchNioHandler(NIOHandler nioHandler) {
    if (nioHandler != null && isIdle() && this.nioHandler == MySQLIdleNIOHandler.INSTANCE) {
      throw new MycatExpection("UNKNOWN state");
    }
    this.nioHandler = nioHandler;
  }

  @Override
  public MySQLSessionManager getSessionManager() {
    return (MySQLSessionManager) sessionManager;
  }

  /**
   * 是否在session管理器中的闲置session池
   */
  public boolean isIdle() {
    return isIdle;
  }

  /**
   * 该方法只能被sessionManager调用
   */
  public void setIdle(boolean idle) {
    isIdle = idle;
  }

  @Override
  public boolean readFromChannel() throws IOException {
    boolean b = MySQLProxySession.super.readFromChannel();
    ProxyBuffer proxyBuffer = this.proxyBuffer;
    MycatMonitor
        .onBackendRead(this, proxyBuffer.currentByteBuffer(), proxyBuffer.channelReadStartIndex(),
            proxyBuffer.channelReadEndIndex()
        );
    return b;
  }

  public boolean isAutomCommit() {
    return (MySQLServerStatusFlags.AUTO_COMMIT & packetResolver.getEofServerStatus()) != 0;
  }


  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  public MySQLIsolation getIsolation() {
    return isolation;
  }

  public void setIsolation(MySQLIsolation isolation) {
    this.isolation = isolation;
  }

  public boolean isRequestSuccess() {
    return requestSuccess;
  }

  public void setRequestSuccess(boolean requestSuccess) {
    this.requestSuccess = requestSuccess;
  }
}
