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

import io.mycat.MycatException;
import io.mycat.beans.MySQLSessionMonopolizeType;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.BackendMySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.replica.MySQLDatasource;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author jamie12221
 *  date 2019-05-07 21:47
 *
 * 后端MySQL Session
 **/
@Getter
@Setter
public class MySQLClientSession extends
    AbstractBackendSession<MySQLClientSession> implements MySQLProxySession<MySQLClientSession> {

  protected final MySQLPacketResolver packetResolver = new BackendMySQLPacketResolver(this);
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

  private long cursorStatementId;
  /**
   * 错误信息
   */
  private String lastMessage;
  private boolean isIdle;

  private String defaultDatabase;
  private String charset;
  private MySQLIsolation isolation;
  private String characterSetResult;
  private long selectLimit = -1;
  private long netWriteTimeout = -1;
  /**
   * 与mycat session绑定的信息 monopolizeType 是无法解绑的原因 TRANSACTION,事务 LOAD_DATA,交换过程
   * PREPARE_STATEMENT_EXECUTE,预处理过程 CURSOR_EXISTS 游标 以上四种情况 mysql客户端的并没有结束对mysql的交互,所以无法解绑
   */
  private MySQLSessionMonopolizeType monopolizeType = MySQLSessionMonopolizeType.NONE;
  private ResponseType responseType;

  /**
   * 构造函数
   */
  public MySQLClientSession(int sessionId,MySQLDatasource datasource,
      NIOHandler nioHandler, SessionManager<MySQLClientSession> sessionManager
  ) {
    super(sessionId,nioHandler, sessionManager);
    Objects.requireNonNull(datasource);
    this.datasource = datasource;
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
      LOGGER.error("channel close occur exception:{}", e);
    }
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
    int serverStatus = getBackendPacketResolver().getServerStatus();
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
    result = 31 * result + (defaultDatabase != null ? defaultDatabase.hashCode() : 0);
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
  public MySQLPacketResolver getBackendPacketResolver() {
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
      throw new MycatException("unsupport max packet {}", MySQLPacketSplitter.MAX_PACKET_SIZE);
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

  @Override
  public long getSelectLimit() {
    return selectLimit;
  }

  public boolean isNoResponse() {
    return noResponse;
  }


  /**
   * 切换出来处理器,在闲置状态中不能设置
   */
  @Override
  public void switchNioHandler(NIOHandler nioHandler) {
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

  public MySQLAutoCommit isAutomCommit() {
    boolean b = (MySQLServerStatusFlags.AUTO_COMMIT & packetResolver.getServerStatus()) != 0;
    return b ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF;
  }

  public boolean isReadOnly() {
    return (MySQLServerStatusFlags.IN_TRANS_READONLY & packetResolver.getServerStatus()) != 0;
  }

  /**
   * 非阻塞nio,向mysql服务器通道写入数据后,通道已经关闭的情况下,会在响应得到写入异常,该标记是确认收到响应不是异常
   */
  public void setRequestSuccess(boolean requestSuccess) {
    this.requestSuccess = requestSuccess;
  }

  public void setMycatSession(MycatSession o) {
    this.mycat = o;
  }


  public void prepareReveiceMultiResultSetResponse() {
    this.getPacketResolver().prepareReveiceMultiResultSetResponse();
  }
}
