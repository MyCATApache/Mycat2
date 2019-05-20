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
package io.mycat.proxy.packet;

import io.mycat.MycatExpection;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.session.MySQLProxySession;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MySQLPacketResolverImpl implements MySQLPacketResolver {

  MySQLPacket mySQLPacket;
  MySQLPacket payload;
  int packetId = 0;
  int head;
  int remainsBytes;
  int startPos;
  int endPos;
  int currentSqlType;
  int length;
  boolean hasResolvePayloadType;
  boolean multiPacket;
  boolean requestFinished;
  boolean isPayloadFinished;
  ComQueryState state = ComQueryState.QUERY_PACKET;
  long statementId;
  int columnCount;
  int prepareOkParametersCount;
  int warningCount;
  int affectedRows;
  int lastInsertId;
  int serverStatus;
  byte okSessionStateInfoType;
  final MySQLProxySession session;
  int capabilityFlags;
  MySQLPayloadType mySQLPacketProcessType;


  public MySQLPacketResolverImpl(MySQLProxySession session) {
    this.session = session;
  }

  @Override
  public final MySQLPayloadType getMySQLPayloadType() {
    return mySQLPacketProcessType;
  }

  @Override
  public final void setMySQLPayloadType(MySQLPayloadType type) {
    this.mySQLPacketProcessType = type;
  }

  @Override
  public final int getPayloadLength() {
    return length;
  }

  @Override
  public final void setPayloadLength(int length) {
    this.length = length;
  }

  @Override
  public final int setPacketId(int packetId) {
    return this.packetId = packetId;
  }

  @Override
  public final int getPacketId() {
    return this.packetId;
  }

  @Override
  public final boolean readFromChannel() throws IOException {
    throw new MycatExpection("");
  }

  @Override
  public final void writeToChannel() throws IOException {
    throw new MycatExpection("");
  }


  @Override
  public final boolean hasResolvePayloadType() {
    return hasResolvePayloadType;
  }

  @Override
  public final void markedResolvePayloadType(boolean marked) {
    hasResolvePayloadType = marked;
  }

  @Override
  public final int getHead() {
    return this.head;
  }

  @Override
  public final int setHead(int head) {
    return this.head = head;
  }

  @Override
  public final int setCurrentComQuerySQLType(int type) {
    return this.currentSqlType = type;
  }

  @Override
  public final int getCurrentSQLType() {
    return this.currentSqlType;
  }

  @Override
  public final int setStartPos(int i) {
    return startPos = i;
  }

  @Override
  public final int getStartPos() {
    return startPos;
  }

  @Override
  public final int setEndPos(int i) {
    return endPos = i;
  }

  @Override
  public final int getEndPos() {
    if (endPos < 0) {
      throw new MycatExpection("");
    }
    return endPos;
  }


  @Override
  public final ComQueryState getState() {
    return this.state;
  }

  @Override
  public final void setState(ComQueryState state) {
    this.state = state;
  }


  @Override
  public final int setColumnCount(int count) {
    return columnCount = count;
  }

  @Override
  public final int getColumnCount() {
    return columnCount;
  }

  @Override
  public final boolean clientDeprecateEof() {
    return MySQLServerCapabilityFlags.isDeprecateEOF(capabilityFlags());
  }

  @Override
  public final int capabilityFlags() {
    return capabilityFlags;
  }

  @Override
  public final void setCapabilityFlags(int serverCapability) {
    this.capabilityFlags = serverCapability;
  }


  @Override
  public final int setRemainsBytes(int remainsBytes) {
    if (remainsBytes < 0) {
      throw new MycatExpection("");
    }
    return this.remainsBytes = remainsBytes;
  }

  @Override
  public final int getRemainsBytes() {
    return this.remainsBytes;
  }

  @Override
  public final boolean setMultiPacket(boolean c) {
    return multiPacket = c;
  }


  @Override
  public final boolean isMultiPacket() {
    return multiPacket;
  }

  @Override
  public final MySQLPacket currentProxybuffer() {
    return (MySQLPacket) session.currentProxyBuffer();
  }

  @Override
  public final void appendPayload(MySQLPacket mySQLPacket, int payloadStartIndex,
      int payloadEndIndex) {
    if (length < 0xffffff) {
      mySQLPacket.packetReadStartIndex(payloadStartIndex);
      mySQLPacket.packetReadEndIndex(getEndPos());
      setPayload(mySQLPacket);
    } else {
      if (this.payload == null) {
        ProxyBufferImpl payload = new ProxyBufferImpl(mySQLPacket.currentBuffer().bufferPool());
        this.payload = payload;
        payload.newBuffer((int) (0xffffff * 1.5));
        ByteBuffer append = mySQLPacket.currentBuffer().currentByteBuffer().duplicate();
        append.position(payloadStartIndex);
        append.limit(payloadEndIndex);
        payload.currentByteBuffer().put(append);
      } else {
        ProxyBuffer payload = (ProxyBuffer) this.payload;
        ByteBuffer byteBuffer = payload.currentByteBuffer().duplicate();
        int length = payloadEndIndex - payloadStartIndex;
        if (byteBuffer.remaining() <= length) {
          int newLength = (int) ((payload.capacity() + length) * 1.5);
          payload.expendToLength(newLength);
        }
        byteBuffer.position(payloadStartIndex);
        byteBuffer.limit(payloadEndIndex);
        payload.currentByteBuffer().put(byteBuffer);
      }
    }
  }

  @Override
  public final void setPayload(MySQLPacket mySQLPacket) {
    this.payload = mySQLPacket;
  }

  @Override
  public final void resetPayload() {
    MySQLPacket mySQLPacket = payload;
    payload = null;
    if (mySQLPacket != null && mySQLPacket != currentProxybuffer()) {
      mySQLPacket.reset();
    }
  }

  @Override
  public final MySQLPacket currentPayload() {
    MySQLPacket mySQLPacket = currentProxybuffer();
    int i = mySQLPacket.packetReadStartIndex();
    if (mySQLPacket == payload) {
      return mySQLPacket;
    } else {
      payload.packetReadStartIndex(0);
      payload.packetReadEndIndex(payload.currentBuffer().currentByteBuffer().position());
    }
    return payload;
  }


  @Override
  public final void setRequestFininshed(boolean b) {
    this.requestFinished = b;
  }

  @Override
  public final void resetCurrentMySQLPacket() {
    if (mySQLPacket != null) {
      mySQLPacket.reset();
    }
    mySQLPacket = null;
  }


  @Override
  public final boolean isPayloadFinished() {
    return isPayloadFinished;
  }

  @Override
  public final boolean setPayloadFinished(boolean b) {
    return isPayloadFinished = b;
  }

  @Override
  public final long getPreparedOkStatementId() {
    return this.statementId;
  }

  @Override
  public void setPreparedOkStatementId(long statementId) {
    this.statementId = statementId;
  }

  @Override
  public final int getPrepareOkColumnsCount() {
    return columnCount;
  }

  @Override
  public final void setPrepareOkColumnsCount(int columnsNumber) {
    this.columnCount = columnsNumber;
  }

  @Override
  public final int getPrepareOkParametersCount() {
    return this.prepareOkParametersCount;
  }

  @Override
  public void setPrepareOkParametersCount(int parametersNumber) {
    this.prepareOkParametersCount = parametersNumber;
  }

  @Override
  public final int getPreparedOkWarningCount() {
    return warningCount;
  }

  @Override
  public final void setPreparedOkWarningCount(int warningCount) {
    this.warningCount = warningCount;
  }


  @Override
  public final int getOkAffectedRows() {
    return this.affectedRows;
  }

  @Override
  public final void setOkAffectedRows(int affectedRows) {
    this.affectedRows = affectedRows;
  }

  @Override
  public final int getOkLastInsertId() {
    return this.lastInsertId;
  }

  @Override
  public final void setOkLastInsertId(int lastInsertId) {
    this.lastInsertId = lastInsertId;
  }

  @Override
  public final int getServerStatus() {
    return serverStatus;
  }

  @Override
  public final int setServerStatus(int serverStatus) {
    return this.serverStatus = serverStatus;
  }

  @Override
  public final int getOkWarningCount() {
    return warningCount;
  }

  @Override
  public final void setOkWarningCount(int warningCount) {
    this.warningCount = warningCount;
  }

  @Override
  public final byte[] getOkStatusInfo() {
    throw new MycatExpection("");
  }

  @Override
  public final void setOkStatusInfo(byte[] statusInfo) {
  }

  @Override
  public final byte getOkSessionStateInfoType() {
    return okSessionStateInfoType;
  }

  @Override
  public final void setOkSessionStateInfoType(byte sessionStateInfoType) {
    this.okSessionStateInfoType = sessionStateInfoType;
  }

  @Override
  public final byte[] getOkSessionStateInfoTypeData() {
    throw new MycatExpection("");
  }

  @Override
  public final void setOkSessionStateInfoTypeData(byte[] sessionStateInfoTypeData) {
  }

  @Override
  public final byte[] getOkMessage() {
    throw new MycatExpection("");
  }

  @Override
  public final void setOkMessage(byte[] message) {
  }

  @Override
  public final int getEofWarningCount() {
    return this.warningCount;
  }

  @Override
  public final void setEofWarningCount(int warningCount) {
    this.warningCount = warningCount;
  }

  @Override
  public final int getEofServerStatus() {
    return this.serverStatus;
  }

  @Override
  public final int setEofServerStatus(int status) {
    return this.serverStatus = status;
  }

}
