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

import io.mycat.MySQLServerStatus;
import io.mycat.MycatExpection;
import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.buffer.BufferPool;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.logTip.SessionTip;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatSessionView;
import io.mycat.proxy.NetMonitor;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.handler.CommandHandler;
import io.mycat.proxy.handler.CommandHandlerAdapter;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolverImpl;
import io.mycat.proxy.task.client.MySQLTaskUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.LinkedList;

public final class MycatSession extends AbstractSession<MycatSession> implements
    MySQLProxySession<MycatSession>, MycatSessionView {

  private final CommandHandler commandHandler;

  public MycatSession(BufferPool bufferPool, Selector selector, SocketChannel channel,
      int socketOpt, NIOHandler nioHandler, SessionManager<MycatSession> sessionManager,
      CommandHandler commandHandler)
      throws IOException {
    super(selector, channel, socketOpt, nioHandler, sessionManager);
    proxyBuffer = new ProxyBufferImpl(bufferPool);
    this.commandHandler = commandHandler;
  }

  /**
   * mysql服务器状态
   */
  private final MySQLServerStatus serverStatus = new MySQLServerStatus();
  /***
   * 以下资源要做session关闭时候释放
   */
  private final ProxyBuffer proxyBuffer;//reset
  private final ByteBuffer header = ByteBuffer.allocate(4);//gc
  private MycatSchema schema;
  private final LinkedList<ByteBuffer> writeQueue = new LinkedList<>();//buffer recycle
  private final MySQLPacketResolver packetResolver = new MySQLPacketResolverImpl(this);//reset


  /**
   * 报文写入辅助类
   */
  private final ByteBuffer[] packetContainer = new ByteBuffer[2];
  private final MySQLPacketSplitter packetSplitter = new PacketSplitterImpl();
  private boolean responseFinished = false;//每次在处理请求时候就需要重置
  private MySQLClientSession backend;//unbind
  private MycatSessionWriteHandler writeHandler = MySQLProxySession.WriteHandler.INSTANCE;
  private AsyncTaskCallBack finallyCallBack;
  /**
   * 路由信息
   */
  private String dataNode;

  public void handle() throws IOException {
    assert commandHandler != null;
    CommandHandlerAdapter.handle(this, commandHandler);
  }


  public void switchWriteHandler(MycatSessionWriteHandler writeHandler) {
    this.writeHandler = writeHandler;
  }

  public void onHandlerFinishedClear(boolean normal) {
    if (backend != null) {
      backend.unbindMycatIfNeed(!normal);
    }
    resetPacket();
    setResponseFinished(false);
    if (finallyCallBack != null) {
      finallyCallBack.finished(this, this, normal, this.getLastMessage(), null);
      finallyCallBack = null;
    }
  }

  public MySQLAutoCommit getAutoCommit() {
    return this.serverStatus.getAutoCommit();
  }

  public void setAutoCommit(MySQLAutoCommit off) {
    this.serverStatus.setAutoCommit(off);
  }

  public MySQLIsolation getIsolation() {
    return this.serverStatus.getIsolation();
  }

  public void setIsolation(MySQLIsolation readUncommitted) {
    this.serverStatus.setIsolation(readUncommitted);
  }

  public void setCharset(int index, String charsetName) {
    this.serverStatus.setCharset(index, charsetName, Charset.forName(charsetName));
  }


  public String getDataNode() {
    return dataNode;
  }

  public void switchDataNode(String dataNode) {
    if (this.backend == null) {
      this.dataNode = dataNode;
      return;
    } else {
      if (dataNode.equals(this.dataNode)) {
        return;
      } else {
        throw new MycatExpection(SessionTip.CANNOT_SWITCH_DATANODE.getMessage());
      }
    }
  }


  public void setSchema(MycatSchema schema) {
    this.schema = schema;
  }

  public MySQLClientSession currentBackend() {
    return backend;
  }

  public void getBackend(boolean runOnSlave, MySQLDataNode dataNode,
      LoadBalanceStrategy strategy, AsyncTaskCallBack<MySQLClientSession> finallyCallBack) {
    this.switchDataNode(dataNode.getName());
    if (this.backend != null) {
      //只要backend还有值,就说明上次命令因为事务或者遇到预处理,loadata这种跨多次命令的类型导致mysql不能释放
      finallyCallBack.finished(this.backend, this, true, null, null);
      return;
    }
    MySQLIsolation isolation = this.getIsolation();
    MySQLAutoCommit autoCommit = this.getAutoCommit();
    String charsetName = this.getCharsetName();
    MySQLTaskUtil
        .getMySQLSession(dataNode, isolation, autoCommit, charsetName,
            runOnSlave,
            strategy, (session, sender, success, result, attr) ->
            {
              if (success) {
                this.switchDataNode(dataNode.getName());
                session.bind(this);
                finallyCallBack.finished(session, sender, true, result, attr);
              } else {
                finallyCallBack.finished(null, sender, false, result, attr);
              }
            });
  }

  @Override
  public void close(boolean normal, String hint) {
    if (!normal) {
      assert hint != null;
      setLastMessage(hint);
      writeErrorEndPacketBySyncInProcessError();
    }
    assert hint != null;
    onHandlerFinishedClear(normal);
    channelKey.cancel();
    closed = true;
    try {
      getSessionManager().removeSession(this, normal, hint);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Override
  public ProxyBuffer currentProxyBuffer() {
    return proxyBuffer;
  }

  @Override
  public MySQLPacketResolver getPacketResolver() {
    return packetResolver;
  }

  @Override
  public void setCurrentProxyBuffer(ProxyBuffer buffer) {
    throw new MycatExpection("unsupport!");
  }

  public int getServerCapabilities() {
    return this.serverStatus.getServerCapabilities();
  }

  public void setServerCapabilities(int serverCapabilities) {
    this.serverStatus.setServerCapabilities(serverCapabilities);
    packetResolver.setCapabilityFlags(serverCapabilities);
  }

  public void bind(MySQLClientSession mySQLSession) {
    this.backend = mySQLSession;
  }

  public long getAffectedRows() {
    return this.serverStatus.getAffectedRows();
  }

  public void setAffectedRows(long affectedRows) {
    this.serverStatus.setAffectedRows(affectedRows);
  }


  @Override
  public LinkedList<ByteBuffer> writeQueue() {
    return writeQueue;
  }

  @Override
  public BufferPool bufferPool() {
    return getMycatReactorThread().getBufPool();
  }

  @Override
  public ByteBuffer packetHeaderBuffer() {
    return header;
  }


  @Override
  public ByteBuffer[] packetContainer() {
    return packetContainer;
  }

  @Override
  public void setPakcetId(int packet) {
    packetResolver.setPacketId(packet);
  }

  @Override
  public byte getNextPacketId() {
    return (byte) packetResolver.incrementPacketIdAndGet();
  }


  @Override
  public MySQLPacketSplitter packetSplitter() {
    return packetSplitter;
  }

  @Override
  public String getLastMessage() {
    String lastMessage = this.serverStatus.getLastMessage();
    return lastMessage + "";
  }

  public String setLastMessage(String lastMessage) {
    this.serverStatus.setLastMessage(lastMessage);
    return lastMessage;
  }

  @Override
  public long affectedRows() {
    return this.serverStatus.getAffectedRows();
  }

  @Override
  public long incrementAffectedRows() {
    return serverStatus.incrementAffectedRows();
  }

  @Override
  public int getServerStatus() {
    return this.serverStatus.getServerStatus();
  }

  @Override
  public int setServerStatus(int s) {
    return this.serverStatus.setServerStatus(s);
  }


  @Override
  public int incrementWarningCount() {
    return this.serverStatus.incrementWarningCount();
  }


  @Override
  public int setLastInsertId(int s) {
    return this.serverStatus.getWarningCount();
  }

  @Override
  public int getLastErrorCode() {
    return this.serverStatus.getWarningCount();
  }

  @Override
  public boolean isDeprecateEOF() {
    return MySQLServerCapabilityFlags.isDeprecateEOF(this.serverStatus.getServerCapabilities());
  }

  public int getWarningCount() {
    return this.serverStatus.getWarningCount();
  }

  public void setWarningCount(int warningCount) {
    this.serverStatus.setWarningCount(warningCount);
  }

  public long getLastInsertId() {
    return this.serverStatus.getLastInsertId();
  }

  public void setLastInsertId(long lastInsertId) {
    this.serverStatus.setLastInsertId(lastInsertId);
  }

  public void resetSession() {
    throw new MycatExpection("unsupport!");
  }

  @Override
  public Charset charset() {
    return this.serverStatus.getCharset();
  }

  public String getCharsetName() {
    return this.serverStatus.getCharsetName();
  }

  @Override
  public int charsetIndex() {
    return this.serverStatus.getCharsetIndex();
  }

  @Override
  public int getCapabilities() {
    return this.serverStatus.getServerCapabilities();
  }

  @Override
  public void setLastErrorCode(int errorCode) {
    this.serverStatus.setLastErrorCode(errorCode);
  }

  @Override
  public boolean isResponseFinished() {
    return responseFinished;
  }

  @Override
  public void setResponseFinished(boolean b) {
    responseFinished = b;
  }

  @Override
  public void switchMySQLServerWriteHandler() {
    this.writeHandler = MySQLServerSession.WriteHandler.INSTANCE;
  }

  @Override
  public void writeToChannel() throws IOException {
    writeHandler.writeToChannel(this);
  }

  public boolean readProxyPayloadFully() {
    return packetResolver.readMySQLPayloadFully();
  }

  @Override
  public final boolean readFromChannel() throws IOException {
    boolean b = MySQLProxySession.super.readFromChannel();
    NetMonitor.onFrontRead(this, proxyBuffer.currentByteBuffer(),
        proxyBuffer.channelReadStartIndex(), proxyBuffer.channelReadEndIndex());
    return b;
  }

  public MySQLPacket currentProxyPayload() {
    return packetResolver.currentPayload();
  }

  public void resetCurrentProxyPayload() {
    packetResolver.resetPayload();
  }

  public void resetPacket() {
    packetResolver.reset();
    for (ByteBuffer byteBuffer : writeQueue) {
      getMycatReactorThread().getBufPool().recycle(byteBuffer);
    }
    writeQueue.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MycatSession that = (MycatSession) o;

    return this.sessionId == that.sessionId;
  }

  @Override
  public int hashCode() {
    return this.sessionId;
  }

  @Override
  public int getLocalInFileState() {
    return this.serverStatus.getLocalInFileRequestState();
  }

  @Override
  public void setLocalInFileState(int value) {
    this.serverStatus.setLocalInFileRequestState(value);
  }

  @Override
  public int getNumParamsByStatementId(long statementId) {
    return 0;
  }

  public AsyncTaskCallBack<MycatSessionView> getCallBack() {
    AsyncTaskCallBack<MycatSessionView> finallyCallBack = this.finallyCallBack;
    this.finallyCallBack = null;
    return finallyCallBack;
  }

  @Override
  public void setCallBack(AsyncTaskCallBack callBack) {
    this.finallyCallBack = callBack;
  }

  @Override
  public void switchNioHandler(NIOHandler nioHandler) {
    this.nioHandler = nioHandler;
  }
}
