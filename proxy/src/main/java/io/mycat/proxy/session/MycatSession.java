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

import io.mycat.MySQLDataNode;
import io.mycat.MycatExpection;
import io.mycat.beans.MySQLServerStatus;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerCapabilityFlags;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.MycatSessionWriteHandler;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.executer.MySQLDataNodeExecuter;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolverImpl;
import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.packet.PacketSplitterImpl;
import io.mycat.proxy.task.AsynTaskCallBack;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MycatSession extends AbstractSession<MycatSession> implements
    MySQLServerSession<MycatSession>, MySQLProxySession<MycatSession> {

  public MycatSession(BufferPool bufferPool, Selector selector, SocketChannel channel,
      int socketOpt, NIOHandler nioHandler, SessionManager<? extends Session> sessionManager)
      throws IOException {
    super(selector, channel, socketOpt, nioHandler, sessionManager);
    proxyBuffer = new ProxyBufferImpl(bufferPool);
  }

  private MycatSchema schema;
  private final ProxyBuffer proxyBuffer;
  private final ByteBuffer header = ByteBuffer.allocate(4);
  final MySQLServerStatus serverStatus = new MySQLServerStatus();
  private MySQLClientSession backend;
  private String dataNode;
  protected final LinkedList<ByteBuffer> writeQueue = new LinkedList<>();
  protected final MySQLPacketResolver packetResolver = new MySQLPacketResolverImpl(this);
  private MycatSessionWriteHandler writeHandler = MySQLProxySession.WriteHandler.INSTANCE;
  private MySQLCommandFinishedType finishedType = MySQLCommandFinishedType.RESPONSE;
  private boolean responseFinished = false;
  final ByteBuffer[] packetContainer = new ByteBuffer[2];
  final PacketSplitter packetSplitter = new PacketSplitterImpl();

  public MySQLCommandFinishedType getCommandFinishedType() {
    return finishedType;
  }

  public void switchWriteHandler(MycatSessionWriteHandler writeHandler) {
    this.writeHandler = writeHandler;
  }

  public void responseFinishedClear() {
    return;
  }

  public void setAutoCommit(MySQLAutoCommit off) {
    this.serverStatus.setAutoCommit(off);
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

  public void setDataNode(String dataNode) {
    this.dataNode = dataNode;
  }


  public MycatSchema getSchema() {
    return schema == null ? schema = MycatRuntime.INSTANCE.getDefaultSchema()
               : schema;
  }

  public void setSchema(MycatSchema schema) {
    this.schema = schema;
  }

  public MySQLClientSession getBackend() {
    return backend;
  }

  public void getBackend(boolean runOnSlave, MySQLDataNode dataNode,
      LoadBalanceStrategy strategy, AsynTaskCallBack<MySQLClientSession> finallyCallBack) {
    if (this.backend != null) {
      //只要backend还有值,就说明上次命令因为事务或者遇到预处理,loadata这种跨多次命令的类型导致mysql不能释放
      finallyCallBack.finished(this.backend, this, true, null, null);
      return;
    }
    MySQLIsolation isolation = this.getIsolation();
    MySQLAutoCommit autoCommit = this.getAutoCommit();
    String charsetName = this.getCharsetName();
    MySQLDataNodeExecuter
        .getMySQLSession(dataNode, isolation, autoCommit, charsetName,
            runOnSlave,
            strategy, (session, sender, success, result, errorMessage) ->
            {
              if (success) {
                session.bind(this);
                finallyCallBack.finished(session, sender, true, result, errorMessage);
              } else {
                finallyCallBack.finished(null, sender, false, result, errorMessage);
              }
            });
  }

  public MySQLAutoCommit getAutoCommit() {
    return this.serverStatus.getAutoCommit();
  }

  public MySQLIsolation getIsolation() {
    return this.serverStatus.getIsolation();
  }

  @Override
  public void close(boolean normal, String hint) {

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

  public String getLastMessage() {
    return this.serverStatus.getLastMessage();
  }

  public void setLastMessage(String lastMessage) {
    this.serverStatus.setLastMessage(lastMessage);
  }

  public void setBackend(MySQLClientSession backend) {
    this.backend = backend;
  }

  public long getAffectedRows() {
    return this.serverStatus.getAffectedRows();
  }

  public void setAffectedRows(long affectedRows) {
    this.serverStatus.setAffectedRows(affectedRows);
  }

  public int getServerStatus() {
    return this.serverStatus.getServerStatus();
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
  public PacketSplitter packetSplitter() {
    return packetSplitter;
  }

  @Override
  public String lastMessage() {
    return this.serverStatus.getLastMessage();
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
  public int serverStatus() {
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
  public int warningCount() {
    return this.serverStatus.getWarningCount();
  }

  @Override
  public long lastInsertId() {
    return this.serverStatus.getWarningCount();
  }

  @Override
  public int setLastInsertId(int s) {
    return this.serverStatus.getWarningCount();
  }

  @Override
  public int lastErrorCode() {
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

  public void useSchema(String schema) {
    this.schema = MycatRuntime.INSTANCE.getSchemaByName(schema);
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
  public int capabilities() {
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
  public void writeToChannel() throws IOException {
    writeHandler.writeToChannel(this);
  }

  public boolean readProxyPayloadFully() throws IOException {
    return packetResolver.readMySQLPayloadFully();
  }

  public MySQLPacket currentProxyPayload() throws IOException {
    return packetResolver.currentPayload();
  }

  public void resetCurrentProxyPayload() throws IOException {
    packetResolver.resetPayload();
  }

  public boolean readPartProxyPayload() throws IOException {
    return packetResolver.readMySQLPacket();
  }

  public void resetPacket() {
    packetResolver.reset();
  }
}
